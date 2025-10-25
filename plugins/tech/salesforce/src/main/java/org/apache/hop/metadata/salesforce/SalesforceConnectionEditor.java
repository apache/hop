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

package org.apache.hop.metadata.salesforce;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class SalesforceConnectionEditor extends MetadataEditor<SalesforceConnection> {
  private static final Class<?> PKG = SalesforceConnectionEditor.class;

  private Text wName;

  // Authentication Type
  private Label wlAuthType;
  private org.eclipse.swt.custom.CCombo wAuthType;

  // Username/Password Group
  private Group wUsernamePasswordGroup;
  private LabelTextVar wUsername;
  private PasswordTextVar wPassword;
  private PasswordTextVar wSecurityToken;
  private LabelTextVar wTargetUrl;

  // OAuth Group
  private Group wOAuthGroup;
  private LabelTextVar wOAuthClientId;
  private PasswordTextVar wOAuthClientSecret;
  private LabelTextVar wOAuthRedirectUri;
  private LabelTextVar wOAuthInstanceUrl;
  private LabelTextVar wOAuthAccessToken;
  private PasswordTextVar wOAuthRefreshToken;
  private Button wOAuthAuthorize;
  private Button wOAuthExchange;

  // OAuth JWT Group
  private Group wOAuthJwtGroup;
  private TextVar wOAuthJwtUsername;
  private TextVar wOAuthJwtConsumerKey;
  private PasswordTextVar wOAuthJwtPrivateKey;
  private TextVar wOAuthJwtTokenEndpoint;

  // Test button
  private Button wTest;

  // PKCE parameters
  private String codeVerifier;
  private String codeChallenge;

  public SalesforceConnectionEditor(
      HopGui hopGui,
      MetadataManager<SalesforceConnection> manager,
      SalesforceConnection salesforceConnection) {
    super(hopGui, manager, salesforceConnection);
  }

  @Override
  public void createControl(Composite composite) {
    PropsUi props = PropsUi.getInstance();

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    IVariables variables = hopGui.getVariables();

    // The name
    Label wlName = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "SalesforceConnectionEditor.Name"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(95, 0);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    // Authentication Type
    wlAuthType = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlAuthType);
    wlAuthType.setText(BaseMessages.getString(PKG, "SalesforceConnectionEditor.AuthType"));
    FormData fdlAuthType = new FormData();
    fdlAuthType.top = new FormAttachment(lastControl, margin);
    fdlAuthType.left = new FormAttachment(0, 0);
    fdlAuthType.right = new FormAttachment(middle, -margin);
    wlAuthType.setLayoutData(fdlAuthType);

    wAuthType = new org.eclipse.swt.custom.CCombo(composite, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wAuthType);
    wAuthType.setItems(
        new String[] {
          BaseMessages.getString(PKG, "SalesforceConnectionEditor.AuthType.UsernamePassword"),
          BaseMessages.getString(PKG, "SalesforceConnectionEditor.AuthType.OAuth"),
          BaseMessages.getString(PKG, "SalesforceConnectionEditor.AuthType.OAuthJWT")
        });
    wAuthType.select(0); // Default to Username/Password
    FormData fdAuthType = new FormData();
    fdAuthType.left = new FormAttachment(middle, 0);
    fdAuthType.top = new FormAttachment(wlAuthType, 0, SWT.CENTER);
    fdAuthType.right = new FormAttachment(95, 0);
    wAuthType.setLayoutData(fdAuthType);
    wAuthType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            updateAuthenticationUI();
          }
        });
    lastControl = wAuthType;

    // Username/Password Group
    wUsernamePasswordGroup = new Group(composite, SWT.SHADOW_ETCHED_IN);
    wUsernamePasswordGroup.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.UsernamePasswordGroup"));
    FormLayout fUsernamePasswordLayout = new FormLayout();
    fUsernamePasswordLayout.marginWidth = 3;
    fUsernamePasswordLayout.marginHeight = 3;
    wUsernamePasswordGroup.setLayout(fUsernamePasswordLayout);
    PropsUi.setLook(wUsernamePasswordGroup);

    // Username
    wUsername =
        new LabelTextVar(
            variables,
            wUsernamePasswordGroup,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.Username"),
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.Username.Tooltip"));
    PropsUi.setLook(wUsername);
    FormData fdUsername = new FormData();
    fdUsername.left = new FormAttachment(0, 0);
    fdUsername.top = new FormAttachment(0, margin);
    fdUsername.right = new FormAttachment(100, 0);
    wUsername.setLayoutData(fdUsername);

    // Password
    Label wlPassword = new Label(wUsernamePasswordGroup, SWT.RIGHT);
    PropsUi.setLook(wlPassword);
    wlPassword.setText(BaseMessages.getString(PKG, "SalesforceConnectionEditor.Password"));
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wUsername, margin);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);

    wPassword =
        new PasswordTextVar(
            variables,
            wUsernamePasswordGroup,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.Password.Tooltip"));
    PropsUi.setLook(wPassword);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.top = new FormAttachment(wlPassword, 0, SWT.CENTER);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    // Security Token (Optional)
    Label wlSecurityToken = new Label(wUsernamePasswordGroup, SWT.RIGHT);
    PropsUi.setLook(wlSecurityToken);
    wlSecurityToken.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.SecurityToken") + " (Optional)");
    FormData fdlSecurityToken = new FormData();
    fdlSecurityToken.left = new FormAttachment(0, 0);
    fdlSecurityToken.top = new FormAttachment(wPassword, margin);
    fdlSecurityToken.right = new FormAttachment(middle, -margin);
    wlSecurityToken.setLayoutData(fdlSecurityToken);

    wSecurityToken =
        new PasswordTextVar(
            variables,
            wUsernamePasswordGroup,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.SecurityToken.Tooltip"));
    PropsUi.setLook(wSecurityToken);
    FormData fdSecurityToken = new FormData();
    fdSecurityToken.left = new FormAttachment(middle, 0);
    fdSecurityToken.top = new FormAttachment(wlSecurityToken, 0, SWT.CENTER);
    fdSecurityToken.right = new FormAttachment(100, 0);
    wSecurityToken.setLayoutData(fdSecurityToken);

    // Target URL
    wTargetUrl =
        new LabelTextVar(
            variables,
            wUsernamePasswordGroup,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.TargetUrl"),
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.TargetUrl.Tooltip"));
    PropsUi.setLook(wTargetUrl);
    FormData fdTargetUrl = new FormData();
    fdTargetUrl.left = new FormAttachment(0, 0);
    fdTargetUrl.top = new FormAttachment(wSecurityToken, margin);
    fdTargetUrl.right = new FormAttachment(100, 0);
    wTargetUrl.setLayoutData(fdTargetUrl);

    FormData fdUsernamePasswordGroup = new FormData();
    fdUsernamePasswordGroup.left = new FormAttachment(0, 0);
    fdUsernamePasswordGroup.right = new FormAttachment(100, 0);
    fdUsernamePasswordGroup.top = new FormAttachment(lastControl, margin);
    wUsernamePasswordGroup.setLayoutData(fdUsernamePasswordGroup);

    // OAuth Group
    wOAuthGroup = new Group(composite, SWT.SHADOW_ETCHED_IN);
    wOAuthGroup.setText(BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthGroup"));
    FormLayout fOAuthLayout = new FormLayout();
    fOAuthLayout.marginWidth = 3;
    fOAuthLayout.marginHeight = 3;
    wOAuthGroup.setLayout(fOAuthLayout);
    PropsUi.setLook(wOAuthGroup);

    // OAuth Client ID
    wOAuthClientId =
        new LabelTextVar(
            variables,
            wOAuthGroup,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthClientId"),
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthClientId.Tooltip"));
    PropsUi.setLook(wOAuthClientId);
    FormData fdOAuthClientId = new FormData();
    fdOAuthClientId.left = new FormAttachment(0, 0);
    fdOAuthClientId.top = new FormAttachment(0, margin);
    fdOAuthClientId.right = new FormAttachment(100, 0);
    wOAuthClientId.setLayoutData(fdOAuthClientId);

    // OAuth Client Secret
    Label wlOAuthClientSecret = new Label(wOAuthGroup, SWT.RIGHT);
    PropsUi.setLook(wlOAuthClientSecret);
    wlOAuthClientSecret.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthClientSecret"));
    FormData fdlOAuthClientSecret = new FormData();
    fdlOAuthClientSecret.left = new FormAttachment(0, 0);
    fdlOAuthClientSecret.top = new FormAttachment(wOAuthClientId, margin);
    fdlOAuthClientSecret.right = new FormAttachment(middle, -margin);
    wlOAuthClientSecret.setLayoutData(fdlOAuthClientSecret);

    wOAuthClientSecret =
        new PasswordTextVar(
            variables,
            wOAuthGroup,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthClientSecret.Tooltip"));
    PropsUi.setLook(wOAuthClientSecret);
    FormData fdOAuthClientSecret = new FormData();
    fdOAuthClientSecret.left = new FormAttachment(middle, 0);
    fdOAuthClientSecret.top = new FormAttachment(wlOAuthClientSecret, 0, SWT.CENTER);
    fdOAuthClientSecret.right = new FormAttachment(100, 0);
    wOAuthClientSecret.setLayoutData(fdOAuthClientSecret);

    // OAuth Redirect URI
    wOAuthRedirectUri =
        new LabelTextVar(
            variables,
            wOAuthGroup,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthRedirectUri"),
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthRedirectUri.Tooltip"));
    PropsUi.setLook(wOAuthRedirectUri);
    FormData fdOAuthRedirectUri = new FormData();
    fdOAuthRedirectUri.left = new FormAttachment(0, 0);
    fdOAuthRedirectUri.top = new FormAttachment(wOAuthClientSecret, margin);
    fdOAuthRedirectUri.right = new FormAttachment(100, 0);
    wOAuthRedirectUri.setLayoutData(fdOAuthRedirectUri);

    // OAuth Instance URL
    wOAuthInstanceUrl =
        new LabelTextVar(
            variables,
            wOAuthGroup,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthInstanceUrl"),
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthInstanceUrl.Tooltip"));
    PropsUi.setLook(wOAuthInstanceUrl);
    FormData fdOAuthInstanceUrl = new FormData();
    fdOAuthInstanceUrl.left = new FormAttachment(0, 0);
    fdOAuthInstanceUrl.top = new FormAttachment(wOAuthRedirectUri, margin);
    fdOAuthInstanceUrl.right = new FormAttachment(100, 0);
    wOAuthInstanceUrl.setLayoutData(fdOAuthInstanceUrl);

    // OAuth Authorize button
    wOAuthAuthorize = new Button(wOAuthGroup, SWT.PUSH);
    wOAuthAuthorize.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthAuthorize"));
    PropsUi.setLook(wOAuthAuthorize);
    FormData fdOAuthAuthorize = new FormData();
    wOAuthAuthorize.setToolTipText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthAuthorize.Tooltip"));
    fdOAuthAuthorize.top = new FormAttachment(wOAuthInstanceUrl, margin);
    fdOAuthAuthorize.right = new FormAttachment(100, 0);
    wOAuthAuthorize.setLayoutData(fdOAuthAuthorize);
    wOAuthAuthorize.addListener(SWT.Selection, e -> authorizeOAuth());

    // OAuth Access Token
    wOAuthAccessToken =
        new LabelTextVar(
            variables,
            wOAuthGroup,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthAccessToken"),
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthAccessToken.Tooltip"),
            true);
    PropsUi.setLook(wOAuthAccessToken);
    FormData fdOAuthAccessToken = new FormData();
    fdOAuthAccessToken.left = new FormAttachment(0, 0);
    fdOAuthAccessToken.top = new FormAttachment(wOAuthAuthorize, margin);
    fdOAuthAccessToken.right = new FormAttachment(100, 0);
    wOAuthAccessToken.setLayoutData(fdOAuthAccessToken);

    // OAuth Refresh Token
    Label wlOAuthRefreshToken = new Label(wOAuthGroup, SWT.RIGHT);
    PropsUi.setLook(wlOAuthRefreshToken);
    wlOAuthRefreshToken.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthRefreshToken"));
    FormData fdlOAuthRefreshToken = new FormData();
    fdlOAuthRefreshToken.left = new FormAttachment(0, 0);
    fdlOAuthRefreshToken.top = new FormAttachment(wOAuthAccessToken, margin);
    fdlOAuthRefreshToken.right = new FormAttachment(middle, -margin);
    wlOAuthRefreshToken.setLayoutData(fdlOAuthRefreshToken);

    wOAuthRefreshToken =
        new PasswordTextVar(
            variables,
            wOAuthGroup,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthRefreshToken.Tooltip"));
    PropsUi.setLook(wOAuthRefreshToken);
    FormData fdOAuthRefreshToken = new FormData();
    fdOAuthRefreshToken.left = new FormAttachment(middle, 0);
    fdOAuthRefreshToken.top = new FormAttachment(wlOAuthRefreshToken, 0, SWT.CENTER);
    fdOAuthRefreshToken.right = new FormAttachment(100, 0);
    wOAuthRefreshToken.setLayoutData(fdOAuthRefreshToken);

    // OAuth Exchange button
    wOAuthExchange = new Button(wOAuthGroup, SWT.PUSH);
    wOAuthExchange.setText(BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthExchange"));
    PropsUi.setLook(wOAuthExchange);
    FormData fdOAuthExchange = new FormData();
    wOAuthExchange.setToolTipText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthExchange.Tooltip"));
    fdOAuthExchange.top = new FormAttachment(wOAuthRefreshToken, margin);
    fdOAuthExchange.right = new FormAttachment(100, 0);
    wOAuthExchange.setLayoutData(fdOAuthExchange);
    wOAuthExchange.addListener(SWT.Selection, e -> exchangeCodeForTokens());

    FormData fdOAuthGroup = new FormData();
    fdOAuthGroup.left = new FormAttachment(0, 0);
    fdOAuthGroup.right = new FormAttachment(100, 0);
    fdOAuthGroup.top = new FormAttachment(lastControl, margin);
    wOAuthGroup.setLayoutData(fdOAuthGroup);

    // Initially hide OAuth group
    wOAuthGroup.setVisible(false);

    // OAuth JWT Group
    wOAuthJwtGroup = new Group(composite, SWT.SHADOW_ETCHED_IN);
    wOAuthJwtGroup.setText(BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtGroup"));
    FormLayout fOAuthJwtLayout = new FormLayout();
    fOAuthJwtLayout.marginWidth = 3;
    fOAuthJwtLayout.marginHeight = 3;
    wOAuthJwtGroup.setLayout(fOAuthJwtLayout);
    PropsUi.setLook(wOAuthJwtGroup);

    // JWT Username
    Label wlOAuthJwtUsername = new Label(wOAuthJwtGroup, SWT.RIGHT);
    PropsUi.setLook(wlOAuthJwtUsername);
    wlOAuthJwtUsername.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtUsername"));
    FormData fdlOAuthJwtUsername = new FormData();
    fdlOAuthJwtUsername.left = new FormAttachment(0, 0);
    fdlOAuthJwtUsername.top = new FormAttachment(0, margin);
    fdlOAuthJwtUsername.right = new FormAttachment(middle, -margin);
    wlOAuthJwtUsername.setLayoutData(fdlOAuthJwtUsername);

    wOAuthJwtUsername = new TextVar(variables, wOAuthJwtGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOAuthJwtUsername.setToolTipText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtUsername.Tooltip"));
    PropsUi.setLook(wOAuthJwtUsername);
    FormData fdOAuthJwtUsername = new FormData();
    fdOAuthJwtUsername.left = new FormAttachment(middle, 0);
    fdOAuthJwtUsername.top = new FormAttachment(wlOAuthJwtUsername, 0, SWT.CENTER);
    fdOAuthJwtUsername.right = new FormAttachment(100, 0);
    wOAuthJwtUsername.setLayoutData(fdOAuthJwtUsername);

    // JWT Consumer Key
    Label wlOAuthJwtConsumerKey = new Label(wOAuthJwtGroup, SWT.RIGHT);
    PropsUi.setLook(wlOAuthJwtConsumerKey);
    wlOAuthJwtConsumerKey.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtConsumerKey"));
    FormData fdlOAuthJwtConsumerKey = new FormData();
    fdlOAuthJwtConsumerKey.left = new FormAttachment(0, 0);
    fdlOAuthJwtConsumerKey.top = new FormAttachment(wOAuthJwtUsername, margin);
    fdlOAuthJwtConsumerKey.right = new FormAttachment(middle, -margin);
    wlOAuthJwtConsumerKey.setLayoutData(fdlOAuthJwtConsumerKey);

    wOAuthJwtConsumerKey =
        new TextVar(variables, wOAuthJwtGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOAuthJwtConsumerKey.setToolTipText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtConsumerKey.Tooltip"));
    PropsUi.setLook(wOAuthJwtConsumerKey);
    FormData fdOAuthJwtConsumerKey = new FormData();
    fdOAuthJwtConsumerKey.left = new FormAttachment(middle, 0);
    fdOAuthJwtConsumerKey.top = new FormAttachment(wlOAuthJwtConsumerKey, 0, SWT.CENTER);
    fdOAuthJwtConsumerKey.right = new FormAttachment(100, 0);
    wOAuthJwtConsumerKey.setLayoutData(fdOAuthJwtConsumerKey);

    // JWT Private Key
    Label wlOAuthJwtPrivateKey = new Label(wOAuthJwtGroup, SWT.RIGHT);
    PropsUi.setLook(wlOAuthJwtPrivateKey);
    wlOAuthJwtPrivateKey.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtPrivateKey"));
    FormData fdlOAuthJwtPrivateKey = new FormData();
    fdlOAuthJwtPrivateKey.left = new FormAttachment(0, 0);
    fdlOAuthJwtPrivateKey.top = new FormAttachment(wOAuthJwtConsumerKey, margin);
    fdlOAuthJwtPrivateKey.right = new FormAttachment(middle, -margin);
    wlOAuthJwtPrivateKey.setLayoutData(fdlOAuthJwtPrivateKey);

    // Composite for private key field and button
    Composite wOAuthJwtPrivateKeyComposite = new Composite(wOAuthJwtGroup, SWT.NONE);
    PropsUi.setLook(wOAuthJwtPrivateKeyComposite);
    FormLayout privateKeyLayout = new FormLayout();
    privateKeyLayout.marginWidth = 0;
    privateKeyLayout.marginHeight = 0;
    wOAuthJwtPrivateKeyComposite.setLayout(privateKeyLayout);
    FormData fdOAuthJwtPrivateKeyComposite = new FormData();
    fdOAuthJwtPrivateKeyComposite.left = new FormAttachment(middle, 0);
    fdOAuthJwtPrivateKeyComposite.top = new FormAttachment(wlOAuthJwtPrivateKey, 0, SWT.CENTER);
    fdOAuthJwtPrivateKeyComposite.right = new FormAttachment(100, 0);
    wOAuthJwtPrivateKeyComposite.setLayoutData(fdOAuthJwtPrivateKeyComposite);

    // Load from file button (create first to reference in password field)
    Button wOAuthJwtLoadKey = new Button(wOAuthJwtPrivateKeyComposite, SWT.PUSH);
    wOAuthJwtLoadKey.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wOAuthJwtLoadKey.setToolTipText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtLoadKey.Tooltip"));
    PropsUi.setLook(wOAuthJwtLoadKey);
    FormData fdOAuthJwtLoadKey = new FormData();
    fdOAuthJwtLoadKey.right = new FormAttachment(100, 0);
    fdOAuthJwtLoadKey.top = new FormAttachment(0, 0);
    wOAuthJwtLoadKey.setLayoutData(fdOAuthJwtLoadKey);
    wOAuthJwtLoadKey.addListener(SWT.Selection, e -> loadPrivateKeyFromFile());

    wOAuthJwtPrivateKey =
        new PasswordTextVar(
            variables,
            wOAuthJwtPrivateKeyComposite,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtPrivateKey.Tooltip"));
    PropsUi.setLook(wOAuthJwtPrivateKey);
    FormData fdOAuthJwtPrivateKey = new FormData();
    fdOAuthJwtPrivateKey.left = new FormAttachment(0, 0);
    fdOAuthJwtPrivateKey.top = new FormAttachment(wOAuthJwtLoadKey, 0, SWT.CENTER);
    fdOAuthJwtPrivateKey.right = new FormAttachment(wOAuthJwtLoadKey, -margin);
    wOAuthJwtPrivateKey.setLayoutData(fdOAuthJwtPrivateKey);

    // JWT Token Endpoint
    Label wlOAuthJwtTokenEndpoint = new Label(wOAuthJwtGroup, SWT.RIGHT);
    PropsUi.setLook(wlOAuthJwtTokenEndpoint);
    wlOAuthJwtTokenEndpoint.setText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtTokenEndpoint"));
    FormData fdlOAuthJwtTokenEndpoint = new FormData();
    fdlOAuthJwtTokenEndpoint.left = new FormAttachment(0, 0);
    fdlOAuthJwtTokenEndpoint.top = new FormAttachment(wOAuthJwtPrivateKeyComposite, margin);
    fdlOAuthJwtTokenEndpoint.right = new FormAttachment(middle, -margin);
    wlOAuthJwtTokenEndpoint.setLayoutData(fdlOAuthJwtTokenEndpoint);

    wOAuthJwtTokenEndpoint =
        new TextVar(variables, wOAuthJwtGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOAuthJwtTokenEndpoint.setToolTipText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtTokenEndpoint.Tooltip"));
    PropsUi.setLook(wOAuthJwtTokenEndpoint);
    FormData fdOAuthJwtTokenEndpoint = new FormData();
    fdOAuthJwtTokenEndpoint.left = new FormAttachment(middle, 0);
    fdOAuthJwtTokenEndpoint.top = new FormAttachment(wlOAuthJwtTokenEndpoint, 0, SWT.CENTER);
    fdOAuthJwtTokenEndpoint.right = new FormAttachment(100, 0);
    wOAuthJwtTokenEndpoint.setLayoutData(fdOAuthJwtTokenEndpoint);

    FormData fdOAuthJwtGroup = new FormData();
    fdOAuthJwtGroup.left = new FormAttachment(0, 0);
    fdOAuthJwtGroup.right = new FormAttachment(100, 0);
    fdOAuthJwtGroup.top = new FormAttachment(lastControl, margin);
    wOAuthJwtGroup.setLayoutData(fdOAuthJwtGroup);

    // Initially hide OAuth JWT group
    wOAuthJwtGroup.setVisible(false);

    // Test button
    wTest = new Button(composite, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "SalesforceConnectionEditor.TestConnection"));
    PropsUi.setLook(wTest);
    FormData fdTest = new FormData();
    wTest.setToolTipText(
        BaseMessages.getString(PKG, "SalesforceConnectionEditor.TestConnection.Tooltip"));
    fdTest.top = new FormAttachment(wOAuthGroup, margin);
    fdTest.right = new FormAttachment(95, 0);
    wTest.setLayoutData(fdTest);
    wTest.addListener(SWT.Selection, e -> testConnection());

    // Load the metadata into the widgets
    setWidgetsContent();

    // Initialize UI state based on authentication type
    updateAuthenticationUI();

    // Clear the changed flag after initial load
    resetChanged();

    // Add modify listeners to all controls to detect changes
    // This will inform the Metadata perspective that this object was modified and needs to be saved
    Control[] controls = {
      wName,
      wAuthType,
      wUsername.getTextWidget(),
      wPassword.getTextWidget(),
      wSecurityToken.getTextWidget(),
      wTargetUrl.getTextWidget(),
      wOAuthClientId.getTextWidget(),
      wOAuthClientSecret.getTextWidget(),
      wOAuthRedirectUri.getTextWidget(),
      wOAuthInstanceUrl.getTextWidget(),
      wOAuthAccessToken.getTextWidget(),
      wOAuthRefreshToken.getTextWidget(),
      wOAuthJwtUsername,
      wOAuthJwtConsumerKey,
      wOAuthJwtPrivateKey.getTextWidget(),
      wOAuthJwtTokenEndpoint
    };
    for (Control control : controls) {
      control.addListener(SWT.Modify, e -> setChanged());
      control.addListener(SWT.Selection, e -> setChanged());
    }
  }

  private void updateAuthenticationUI() {
    int selectedIndex = wAuthType == null ? 0 : wAuthType.getSelectionIndex();
    boolean isUsernamePassword = (selectedIndex == 0);
    boolean isOAuth = (selectedIndex == 1);
    boolean isOAuthJwt = (selectedIndex == 2);

    // Show/hide appropriate sections
    if (wUsernamePasswordGroup != null) {
      wUsernamePasswordGroup.setVisible(isUsernamePassword);
      if (isUsernamePassword) {
        wUsernamePasswordGroup.layout(true, true);
      }
      ((FormData) wUsernamePasswordGroup.getLayoutData()).height =
          isUsernamePassword ? SWT.DEFAULT : 0;
    }
    if (wOAuthGroup != null) {
      wOAuthGroup.setVisible(isOAuth);
      if (isOAuth) {
        wOAuthGroup.layout(true, true);
      }
      ((FormData) wOAuthGroup.getLayoutData()).height = isOAuth ? SWT.DEFAULT : 0;
    }
    if (wOAuthJwtGroup != null) {
      wOAuthJwtGroup.setVisible(isOAuthJwt);
      if (isOAuthJwt) {
        wOAuthJwtGroup.layout(true, true);
      }
      ((FormData) wOAuthJwtGroup.getLayoutData()).height = isOAuthJwt ? SWT.DEFAULT : 0;
    }

    // Pre-fill URL field with default value when username/password authentication is selected
    if (isUsernamePassword && wTargetUrl != null) {
      String currentUrl = wTargetUrl.getText();
      // Only set default URL if the field is empty (new connection or user hasn't entered anything)
      if (currentUrl == null || currentUrl.trim().isEmpty()) {
        wTargetUrl.setText("https://login.salesforce.com/services/Soap/u/64.0");
        setChanged(); // Mark as changed so user can see the field was updated
      }
    }

    // Pre-fill JWT token endpoint with default value
    if (isOAuthJwt && wOAuthJwtTokenEndpoint != null) {
      String currentEndpoint = wOAuthJwtTokenEndpoint.getText();
      if (currentEndpoint == null || currentEndpoint.trim().isEmpty()) {
        wOAuthJwtTokenEndpoint.setText("https://login.salesforce.com");
        setChanged();
      }
    }

    // Update test button positioning - always attach to the active group
    if (wTest != null) {
      FormData fdTest = (FormData) wTest.getLayoutData();
      if (isOAuth) {
        fdTest.top = new FormAttachment(wOAuthGroup, PropsUi.getMargin());
      } else if (isOAuthJwt) {
        fdTest.top = new FormAttachment(wOAuthJwtGroup, PropsUi.getMargin());
      } else {
        fdTest.top = new FormAttachment(wUsernamePasswordGroup, PropsUi.getMargin());
      }
      wTest.setLayoutData(fdTest);
    }

    // Force complete layout refresh of parent composite
    if (wTest != null && wTest.getParent() != null) {
      Composite parent = wTest.getParent();
      parent.layout(true, true);
      parent.redraw();
      parent.update();
    }
  }

  private boolean isOAuthSelected() {
    if (wAuthType == null) {
      return false;
    }
    return wAuthType.getSelectionIndex() == 1; // OAuth is index 1
  }

  private void authorizeOAuth() {
    try {
      // Get OAuth configuration
      String clientId = wOAuthClientId.getText();
      String redirectUri = wOAuthRedirectUri.getText();
      String instanceUrl = wOAuthInstanceUrl.getText();

      if (Utils.isEmpty(clientId)) {
        MessageBox clientIdMb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        clientIdMb.setMessage("OAuth Client ID is required for authorization.");
        clientIdMb.setText("OAuth Configuration Error");
        clientIdMb.open();
        return;
      }

      if (Utils.isEmpty(redirectUri)) {
        MessageBox redirectUriMb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        redirectUriMb.setMessage("OAuth Redirect URI is required for authorization.");
        redirectUriMb.setText("OAuth Configuration Error");
        redirectUriMb.open();
        return;
      }

      if (Utils.isEmpty(instanceUrl)) {
        MessageBox instanceUrlMb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        instanceUrlMb.setMessage("OAuth Instance URL is required for authorization.");
        instanceUrlMb.setText("OAuth Configuration Error");
        instanceUrlMb.open();
        return;
      }

      // Build authorization URL
      String authUrl = buildAuthorizationUrl(clientId, redirectUri, instanceUrl, false);

      // Show authorization URL in a dialog for manual copying
      showAuthorizationUrlDialog(authUrl, redirectUri);

    } catch (Exception e) {
      MessageBox errorMb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
      errorMb.setMessage("Error starting OAuth authorization: " + e.getMessage());
      errorMb.setText("OAuth Authorization Error");
      errorMb.open();
    }
  }

  private String buildAuthorizationUrl(
      String clientId, String redirectUri, String instanceUrl, boolean forceConsent) {
    try {
      // Generate PKCE parameters
      generatePKCEParameters();

      StringBuilder url = new StringBuilder();
      url.append(instanceUrl);
      if (!instanceUrl.endsWith("/")) {
        url.append("/");
      }
      url.append("services/oauth2/authorize?");
      url.append("response_type=code&");
      url.append("client_id=").append(URLEncoder.encode(clientId, "UTF-8")).append("&");
      url.append("redirect_uri=").append(URLEncoder.encode(redirectUri, "UTF-8")).append("&");
      url.append("scope=").append(URLEncoder.encode("api refresh_token", "UTF-8")).append("&");
      url.append("code_challenge=").append(URLEncoder.encode(codeChallenge, "UTF-8")).append("&");
      url.append("code_challenge_method=S256");

      // Add prompt=consent only if force consent is requested
      if (forceConsent) {
        url.append("&prompt=consent");
      }

      return url.toString();
    } catch (Exception e) {
      throw new RuntimeException("Failed to build authorization URL: " + e.getMessage(), e);
    }
  }

  private void generatePKCEParameters() {
    try {
      // Generate code verifier (43-128 characters, URL-safe)
      SecureRandom secureRandom = new SecureRandom();
      byte[] randomBytes = new byte[32];
      secureRandom.nextBytes(randomBytes);
      codeVerifier = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);

      // Generate code challenge (SHA256 hash of code verifier)
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(codeVerifier.getBytes("UTF-8"));
      codeChallenge = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);

    } catch (Exception e) {
      throw new RuntimeException("Failed to generate PKCE parameters: " + e.getMessage(), e);
    }
  }

  private void showAuthorizationUrlDialog(String authUrl, String redirectUri) {
    // Create a custom dialog to show the authorization URL
    org.eclipse.swt.widgets.Shell urlDialog =
        new org.eclipse.swt.widgets.Shell(getShell(), SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL);
    urlDialog.setText("OAuth Authorization URL");

    // Set dialog size
    org.eclipse.swt.graphics.Rectangle screenSize =
        getShell().getDisplay().getPrimaryMonitor().getBounds();
    int dialogWidth = Math.min(screenSize.width / 2, 800);
    urlDialog.setSize(dialogWidth, 450);

    org.eclipse.swt.layout.GridLayout layout = new org.eclipse.swt.layout.GridLayout();
    layout.marginWidth = 10;
    layout.marginHeight = 10;
    layout.numColumns = 1;
    urlDialog.setLayout(layout);

    // Instructions label
    Label instructionsLabel = new Label(urlDialog, SWT.WRAP);
    instructionsLabel.setText(
        "OAuth Authorization Instructions:\n\n"
            + "1. Click 'Open in Browser' to open the authorization URL\n"
            + "2. Log in to Salesforce and authorize the application\n"
            + "3. After authorization, you'll be redirected to: "
            + redirectUri
            + "\n"
            + "4. IMPORTANT: The authorization code may be hidden in the address bar\n"
            + "   • Click on the address bar to see the full URL\n"
            + "   • Look for the 'code=' parameter in the URL\n"
            + "5. Copy the entire code value (everything after 'code=')\n"
            + "6. Paste the code in the 'Access Token' field above\n"
            + "7. Click 'Exchange Code for Tokens' to get your tokens\n\n"
            + "⚠️  Authorization codes expire in 10 minutes - work quickly!");
    org.eclipse.swt.layout.GridData gdInstructions = new org.eclipse.swt.layout.GridData();
    gdInstructions.horizontalAlignment = org.eclipse.swt.layout.GridData.FILL;
    gdInstructions.grabExcessHorizontalSpace = true;
    instructionsLabel.setLayoutData(gdInstructions);

    // Force consent checkbox
    Button forceConsentCheckbox = new Button(urlDialog, SWT.CHECK);
    forceConsentCheckbox.setText("Force consent (prompt=consent)");
    forceConsentCheckbox.setToolTipText(
        "Force Salesforce to show the authorization page even if previously authorized");
    org.eclipse.swt.layout.GridData gdForceConsent = new org.eclipse.swt.layout.GridData();
    gdForceConsent.horizontalAlignment = org.eclipse.swt.layout.GridData.FILL;
    gdForceConsent.grabExcessHorizontalSpace = true;
    forceConsentCheckbox.setLayoutData(gdForceConsent);

    // URL text area
    Text urlText =
        new Text(urlDialog, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.RESIZE | SWT.WRAP);
    urlText.setText(authUrl);
    urlText.setEditable(false);
    org.eclipse.swt.layout.GridData gdUrlText = new org.eclipse.swt.layout.GridData();
    gdUrlText.horizontalAlignment = org.eclipse.swt.layout.GridData.FILL;
    gdUrlText.grabExcessHorizontalSpace = false;
    gdUrlText.heightHint = 100;
    urlText.setLayoutData(gdUrlText);

    // Add listener to regenerate URL when force consent is toggled
    forceConsentCheckbox.addListener(
        SWT.Selection,
        e -> {
          try {
            String clientId = wOAuthClientId.getText();
            String redirectUriValue = wOAuthRedirectUri.getText();
            String instanceUrl = wOAuthInstanceUrl.getText();
            boolean forceConsent = forceConsentCheckbox.getSelection();
            String newAuthUrl =
                buildAuthorizationUrl(clientId, redirectUriValue, instanceUrl, forceConsent);
            urlText.setText(newAuthUrl);
          } catch (Exception ex) {
            // If there's an error, just keep the current URL
          }
        });

    // Buttons
    Button copyButton = new Button(urlDialog, SWT.PUSH);
    copyButton.setText("Copy URL");
    org.eclipse.swt.layout.GridData gdCopyButton = new org.eclipse.swt.layout.GridData();
    gdCopyButton.horizontalAlignment = org.eclipse.swt.layout.GridData.BEGINNING;
    copyButton.setLayoutData(gdCopyButton);
    copyButton.addListener(
        SWT.Selection,
        e -> {
          urlText.selectAll();
          urlText.copy();
          MessageBox mb = new MessageBox(urlDialog, SWT.OK | SWT.ICON_INFORMATION);
          mb.setMessage("URL copied to clipboard!");
          mb.setText("Copied");
          mb.open();
        });

    Button openButton = new Button(urlDialog, SWT.PUSH);
    openButton.setText("Open in Browser");
    org.eclipse.swt.layout.GridData gdOpenButton = new org.eclipse.swt.layout.GridData();
    gdOpenButton.horizontalAlignment = org.eclipse.swt.layout.GridData.BEGINNING;
    openButton.setLayoutData(gdOpenButton);
    openButton.addListener(
        SWT.Selection,
        e -> {
          try {
            openBrowser(authUrl);
          } catch (Exception ex) {
            MessageBox mb = new MessageBox(urlDialog, SWT.OK | SWT.ICON_ERROR);
            mb.setMessage("Failed to open browser: " + ex.getMessage());
            mb.setText("Error");
            mb.open();
          }
        });

    Button closeButton = new Button(urlDialog, SWT.PUSH);
    closeButton.setText("Close");
    org.eclipse.swt.layout.GridData gdCloseButton = new org.eclipse.swt.layout.GridData();
    gdCloseButton.horizontalAlignment = org.eclipse.swt.layout.GridData.END;
    closeButton.setLayoutData(gdCloseButton);
    closeButton.addListener(SWT.Selection, e -> urlDialog.close());

    // Center the dialog
    urlDialog.pack();
    urlDialog.setLocation(
        getShell().getLocation().x + (getShell().getSize().x - urlDialog.getSize().x) / 2,
        getShell().getLocation().y + (getShell().getSize().y - urlDialog.getSize().y) / 2);

    urlDialog.open();
  }

  private void openBrowser(String url) throws Exception {
    String os = System.getProperty("os.name").toLowerCase();
    Runtime runtime = Runtime.getRuntime();

    if (os.contains("win")) {
      runtime.exec("rundll32 url.dll,FileProtocolHandler " + url);
    } else if (os.contains("mac")) {
      runtime.exec("open " + url);
    } else if (os.contains("nix") || os.contains("nux")) {
      runtime.exec("xdg-open " + url);
    } else {
      throw new Exception("Unsupported operating system: " + os);
    }
  }

  private void exchangeCodeForTokens() {
    try {
      // Get OAuth configuration
      String clientId = wOAuthClientId.getText();
      String clientSecret = wOAuthClientSecret.getText();
      String redirectUri = wOAuthRedirectUri.getText();
      String instanceUrl = wOAuthInstanceUrl.getText();
      String authorizationCode = wOAuthAccessToken.getText(); // User pastes code here

      if (Utils.isEmpty(clientId)) {
        MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        mb.setMessage("OAuth Client ID is required for token exchange.");
        mb.setText("OAuth Configuration Error");
        mb.open();
        return;
      }

      if (Utils.isEmpty(clientSecret)) {
        MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        mb.setMessage("OAuth Client Secret is required for token exchange.");
        mb.setText("OAuth Configuration Error");
        mb.open();
        return;
      }

      if (Utils.isEmpty(redirectUri)) {
        MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        mb.setMessage("OAuth Redirect URI is required for token exchange.");
        mb.setText("OAuth Configuration Error");
        mb.open();
        return;
      }

      if (Utils.isEmpty(instanceUrl)) {
        MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        mb.setMessage("OAuth Instance URL is required for token exchange.");
        mb.setText("OAuth Configuration Error");
        mb.open();
        return;
      }

      if (Utils.isEmpty(authorizationCode)) {
        MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            "Authorization code is required. Please paste the code from the authorization URL.");
        mb.setText("OAuth Configuration Error");
        mb.open();
        return;
      }

      // URL decode the authorization code
      String decodedCode = URLDecoder.decode(authorizationCode, "UTF-8");

      // Build token endpoint URL
      String tokenUrl = instanceUrl;
      if (!tokenUrl.endsWith("/")) {
        tokenUrl += "/";
      }
      tokenUrl += "services/oauth2/token";

      // Prepare the request
      URL url = new URL(tokenUrl);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      connection.setDoOutput(true);

      // Build the request body
      StringBuilder requestBody = new StringBuilder();
      requestBody.append("grant_type=authorization_code&");
      requestBody.append("client_id=").append(URLEncoder.encode(clientId, "UTF-8")).append("&");
      requestBody
          .append("client_secret=")
          .append(URLEncoder.encode(clientSecret, "UTF-8"))
          .append("&");
      requestBody
          .append("redirect_uri=")
          .append(URLEncoder.encode(redirectUri, "UTF-8"))
          .append("&");
      requestBody.append("code=").append(URLEncoder.encode(decodedCode, "UTF-8")).append("&");
      requestBody.append("code_verifier=").append(URLEncoder.encode(codeVerifier, "UTF-8"));

      // Send the request
      try (OutputStream os = connection.getOutputStream()) {
        byte[] input = requestBody.toString().getBytes("UTF-8");
        os.write(input, 0, input.length);
      }

      // Read the response
      int responseCode = connection.getResponseCode();
      BufferedReader reader;
      if (responseCode >= 200 && responseCode < 300) {
        reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      } else {
        reader = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
      }

      StringBuilder response = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line);
      }
      reader.close();

      if (responseCode >= 200 && responseCode < 300) {
        // Parse the JSON response to extract tokens
        String responseStr = response.toString();
        String accessToken = extractJsonValue(responseStr, "access_token");
        String refreshToken = extractJsonValue(responseStr, "refresh_token");
        String instanceUrlFromResponse = extractJsonValue(responseStr, "instance_url");

        // Populate the fields
        wOAuthAccessToken.setText(accessToken);
        wOAuthRefreshToken.setText(refreshToken);
        if (!Utils.isEmpty(instanceUrlFromResponse)) {
          wOAuthInstanceUrl.setText(instanceUrlFromResponse);
        }

        MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage("Tokens successfully obtained and populated!");
        mb.setText("Success");
        mb.open();
      } else {
        MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            "Token exchange failed with response code "
                + responseCode
                + ": "
                + response.toString());
        mb.setText("Token Exchange Error");
        mb.open();
      }

    } catch (Exception e) {
      MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_ERROR);
      mb.setMessage("Error exchanging code for tokens: " + e.getMessage());
      mb.setText("Token Exchange Error");
      mb.open();
    }
  }

  private String extractJsonValue(String json, String key) {
    try {
      String pattern = "\"" + key + "\"\\s*:\\s*\"([^\"]+)\"";
      java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
      java.util.regex.Matcher m = p.matcher(json);
      if (m.find()) {
        return m.group(1);
      }
    } catch (Exception e) {
      // Ignore parsing errors
    }
    return "";
  }

  private void loadPrivateKeyFromFile() {
    try {
      FileDialog dialog = new FileDialog(getShell(), SWT.OPEN);
      dialog.setText(
          BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtLoadKey.DialogTitle"));
      dialog.setFilterExtensions(new String[] {"*.key;*.pem", "*.*"});
      dialog.setFilterNames(
          new String[] {
            BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtLoadKey.FileTypes"),
            BaseMessages.getString(PKG, "System.FileType.AllFiles")
          });

      String filename = dialog.open();
      if (filename != null) {
        // Read the private key file
        java.nio.file.Path path = java.nio.file.Paths.get(filename);
        String keyContent = new String(java.nio.file.Files.readAllBytes(path), "UTF-8");

        // Validate it looks like a private key
        if (keyContent.contains("BEGIN") && keyContent.contains("PRIVATE KEY")) {
          wOAuthJwtPrivateKey.setText(keyContent);
          setChanged();
        } else {
          MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_WARNING);
          mb.setMessage(
              BaseMessages.getString(
                  PKG, "SalesforceConnectionEditor.OAuthJwtLoadKey.InvalidFormat"));
          mb.setText(BaseMessages.getString(PKG, "System.Dialog.Warning.Title"));
          mb.open();
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
          BaseMessages.getString(PKG, "SalesforceConnectionEditor.OAuthJwtLoadKey.Error"),
          e);
    }
  }

  private void testConnection() {
    try {
      // Save current UI values to metadata object before testing
      getWidgetsContent(getMetadata());

      getMetadata().testConnection(hopGui.getVariables(), hopGui.getLog());

      MessageBox mb = new MessageBox(getShell(), SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage("Connection test successful!");
      mb.setText("Success");
      mb.open();
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Connection test failed", e);
    }
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(metadata.getName(), ""));

    // Set authentication type
    if (metadata.isOAuthAuthentication()) {
      wAuthType.select(1);
    } else if (metadata.isOAuthJwtAuthentication()) {
      wAuthType.select(2);
    } else {
      wAuthType.select(0);
    }

    // Username/Password fields
    wUsername.setText(Const.NVL(metadata.getUsername(), ""));
    wPassword.setText(Const.NVL(metadata.getPassword(), ""));
    wSecurityToken.setText(Const.NVL(metadata.getSecurityToken(), ""));
    wTargetUrl.setText(Const.NVL(metadata.getTargetUrl(), ""));

    // OAuth fields
    wOAuthClientId.setText(Const.NVL(metadata.getOauthClientId(), ""));
    wOAuthClientSecret.setText(Const.NVL(metadata.getOauthClientSecret(), ""));
    wOAuthRedirectUri.setText(
        Const.NVL(metadata.getOauthRedirectUri(), "http://localhost:8080/callback"));
    wOAuthInstanceUrl.setText(Const.NVL(metadata.getOauthInstanceUrl(), ""));
    wOAuthAccessToken.setText(Const.NVL(metadata.getOauthAccessToken(), ""));
    wOAuthRefreshToken.setText(Const.NVL(metadata.getOauthRefreshToken(), ""));

    // OAuth JWT fields
    wOAuthJwtUsername.setText(Const.NVL(metadata.getOauthJwtUsername(), ""));
    wOAuthJwtConsumerKey.setText(Const.NVL(metadata.getOauthJwtConsumerKey(), ""));

    // Private Key - show masked placeholder if key exists
    String privateKey = metadata.getOauthJwtPrivateKey();
    if (!Utils.isEmpty(privateKey)) {
      // Show masked indicator instead of actual key for security
      // Use 150 characters to fill the field and indicate substantial key length
      String maskedPlaceholder =
          "••••••••••••••••••••••••••••••••••••••••••••••••••"
              + "••••••••••••••••••••••••••••••••••••••••••••••••••"
              + "••••••••••••••••••••••••••••••••••••••••••••••••••";
      wOAuthJwtPrivateKey.setText(maskedPlaceholder);
      // Store actual key in widget data for later retrieval
      wOAuthJwtPrivateKey.setData("actualPrivateKey", privateKey);
    } else {
      wOAuthJwtPrivateKey.setText("");
      wOAuthJwtPrivateKey.setData("actualPrivateKey", null);
    }

    wOAuthJwtTokenEndpoint.setText(
        Const.NVL(metadata.getOauthJwtTokenEndpoint(), "https://login.salesforce.com"));

    // Update UI and force layout refresh
    updateAuthenticationUI();

    // Force a second layout pass to ensure everything is properly positioned
    if (wTest != null && wTest.getParent() != null) {
      wTest
          .getParent()
          .getDisplay()
          .asyncExec(
              () -> {
                if (!wTest.isDisposed() && !wTest.getParent().isDisposed()) {
                  wTest.getParent().layout(true, true);
                }
              });
    }
  }

  @Override
  public void getWidgetsContent(SalesforceConnection connection) {
    connection.setName(wName.getText());

    int selectedIndex = wAuthType.getSelectionIndex();
    if (selectedIndex == 1) {
      connection.setAuthenticationType("OAUTH");
    } else if (selectedIndex == 2) {
      connection.setAuthenticationType("OAUTH_JWT");
    } else {
      connection.setAuthenticationType("USERNAME_PASSWORD");
    }

    // Username/Password fields
    connection.setUsername(wUsername.getText());
    connection.setPassword(wPassword.getText());
    connection.setSecurityToken(wSecurityToken.getText());
    connection.setTargetUrl(wTargetUrl.getText());

    // OAuth fields
    connection.setOauthClientId(wOAuthClientId.getText());
    connection.setOauthClientSecret(wOAuthClientSecret.getText());
    connection.setOauthRedirectUri(wOAuthRedirectUri.getText());
    connection.setOauthInstanceUrl(wOAuthInstanceUrl.getText());
    connection.setOauthAccessToken(wOAuthAccessToken.getText());
    connection.setOauthRefreshToken(wOAuthRefreshToken.getText());

    // OAuth JWT fields
    connection.setOauthJwtUsername(wOAuthJwtUsername.getText());
    connection.setOauthJwtConsumerKey(wOAuthJwtConsumerKey.getText());

    // Private Key - check if it's the masked placeholder
    String displayedKey = wOAuthJwtPrivateKey.getText();
    // Check if the field contains only bullet characters (masked placeholder)
    if (displayedKey != null && displayedKey.matches("^•+$")) {
      // Use the actual key stored in widget data (unchanged)
      String actualKey = (String) wOAuthJwtPrivateKey.getData("actualPrivateKey");
      connection.setOauthJwtPrivateKey(actualKey != null ? actualKey : "");
    } else {
      // New key was entered/loaded
      connection.setOauthJwtPrivateKey(displayedKey);
    }

    connection.setOauthJwtTokenEndpoint(wOAuthJwtTokenEndpoint.getText());
  }
}
