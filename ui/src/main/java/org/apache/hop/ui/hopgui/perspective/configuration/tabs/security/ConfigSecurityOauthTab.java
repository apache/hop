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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs.security;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopUserStore;
import org.apache.hop.core.security.oidc.HopOidcClient;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigSecurityTab;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;

/** OAuth2 / OpenID Connect settings (mode OAUTH2). */
@GuiPlugin
public class ConfigSecurityOauthTab implements ISecurityConfigSection {

  private Text wOauthIssuer;
  private Text wOauthClientId;
  private Text wOauthClientSecret;
  private Text wOauthRedirectUri;
  private Text wOauthScopes;
  private Text wOauthRoleClaim;
  private Text wOauthUsernameClaim;
  private Button wOauthPkce;

  public ConfigSecurityOauthTab() {
    // Instantiated by ConfigSecurityTab / @GuiTab system
  }

  @GuiTab(
      id = "10302-security-oauth",
      parentId = ConfigSecurityTab.SECURITY_CONFIG_TABS,
      description = "OAuth2 / OpenID Connect")
  public void addOauthTab(CTabFolder wTabFolder) {
    int margin = PropsUi.getMargin();
    int mid = PropsUi.getInstance().getMiddlePct();
    Composite content =
        SecurityConfigUi.createTabContent(wTabFolder, "ConfigSecurityTab.Oauth.Tab");

    Control last = SecurityConfigUi.addHint(content, null, "ConfigSecurityTab.Oauth.Hint", margin);

    wOauthIssuer =
        SecurityConfigUi.addLabeledText(
            content, "ConfigSecurityTab.Oauth.Issuer", last, margin * 2, mid);
    last = wOauthIssuer;

    wOauthClientId =
        SecurityConfigUi.addLabeledText(
            content, "ConfigSecurityTab.Oauth.ClientId", last, margin, mid);
    last = wOauthClientId;

    wOauthClientSecret =
        SecurityConfigUi.addLabeledText(
            content, "ConfigSecurityTab.Oauth.ClientSecret", last, margin, mid);
    wOauthClientSecret.setEchoChar('*');
    last = wOauthClientSecret;

    wOauthRedirectUri =
        SecurityConfigUi.addLabeledText(
            content, "ConfigSecurityTab.Oauth.RedirectUri", last, margin, mid);
    last = wOauthRedirectUri;

    wOauthScopes =
        SecurityConfigUi.addLabeledText(
            content, "ConfigSecurityTab.Oauth.Scopes", last, margin, mid);
    last = wOauthScopes;

    wOauthRoleClaim =
        SecurityConfigUi.addLabeledText(
            content, "ConfigSecurityTab.Oauth.RoleClaim", last, margin, mid);
    last = wOauthRoleClaim;

    wOauthUsernameClaim =
        SecurityConfigUi.addLabeledText(
            content, "ConfigSecurityTab.Oauth.UsernameClaim", last, margin, mid);
    last = wOauthUsernameClaim;

    wOauthPkce = new Button(content, SWT.CHECK);
    PropsUi.setLook(wOauthPkce);
    wOauthPkce.setText(
        org.apache.hop.i18n.BaseMessages.getString(
            ConfigSecurityTab.class, "ConfigSecurityTab.Oauth.Pkce"));
    FormData fdPkce = new FormData();
    fdPkce.left = new FormAttachment(mid, margin);
    fdPkce.top = new FormAttachment(last, margin);
    wOauthPkce.setLayoutData(fdPkce);
    last = wOauthPkce;

    SecurityConfigUi.addHint(content, last, "ConfigSecurityTab.Oauth.MappingsHint", margin * 2);
    SecurityConfigUi.finishTabLayout(content);
  }

  @Override
  public void loadFrom(HopSecurityConfig config, HopUserStore store) {
    if (wOauthIssuer == null || wOauthIssuer.isDisposed()) {
      return;
    }
    wOauthIssuer.setText(Const.NVL(config.getOauthIssuerUrl(), ""));
    wOauthClientId.setText(Const.NVL(config.getOauthClientId(), ""));
    wOauthClientSecret.setText(Const.NVL(config.getOauthClientSecret(), ""));
    wOauthRedirectUri.setText(Const.NVL(config.getOauthRedirectUri(), ""));
    wOauthScopes.setText(Const.NVL(config.getOauthScopes(), "openid profile email"));
    wOauthRoleClaim.setText(Const.NVL(config.getOauthRoleClaim(), "groups"));
    wOauthUsernameClaim.setText(Const.NVL(config.getOauthUsernameClaim(), "preferred_username"));
    wOauthPkce.setSelection(config.isOauthUsePkce());
  }

  @Override
  public void applyTo(HopSecurityConfig config) {
    if (wOauthIssuer == null || wOauthIssuer.isDisposed()) {
      return;
    }
    config.setOauthIssuerUrl(wOauthIssuer.getText().trim());
    config.setOauthClientId(wOauthClientId.getText().trim());
    config.setOauthClientSecret(wOauthClientSecret.getText());
    config.setOauthRedirectUri(wOauthRedirectUri.getText().trim());
    config.setOauthScopes(wOauthScopes.getText().trim());
    config.setOauthRoleClaim(wOauthRoleClaim.getText().trim());
    config.setOauthUsernameClaim(wOauthUsernameClaim.getText().trim());
    config.setOauthUsePkce(wOauthPkce.getSelection());
    HopOidcClient.clearDiscoveryCache();
  }
}
