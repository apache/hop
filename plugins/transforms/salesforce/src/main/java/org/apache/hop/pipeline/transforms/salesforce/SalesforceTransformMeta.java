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

package org.apache.hop.pipeline.transforms.salesforce;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
public abstract class SalesforceTransformMeta<
        Main extends SalesforceTransform, Data extends SalesforceTransformData>
    extends BaseTransformMeta<Main, Data> {

  private static final Class<?> PKG = SalesforceTransformMeta.class;

  /** The Salesforce Target URL */
  @HopMetadataProperty(key = "targetUrl", injectionKey = "SALESFORCE_URL")
  //  @Injection(name = "SALESFORCE_URL")
  private String targetUrl;

  /** The userName */
  @HopMetadataProperty(key = "username", injectionKey = "SALESFORCE_USERNAME")
  //  @Injection(name = "SALESFORCE_USERNAME")
  private String username;

  //  /** Authentication type: USERNAME_PASSWORD or OAUTH */
  //  @HopMetadataProperty(key = "", injectionKey = "USERNAME_PASSWORD")
  //  @Injection(name = "AUTHENTICATION_TYPE")
  //  private String authenticationType = "USERNAME_PASSWORD"; // Default for backward compatibility
  //
  //  /** OAuth Client ID */
  //  @Injection(name = "OAUTH_CLIENT_ID")
  //  private String oauthClientId;
  //
  //  /** OAuth Client Secret */
  //  @Injection(name = "OAUTH_CLIENT_SECRET")
  //  private String oauthClientSecret;
  //
  //  /** OAuth Redirect URI */
  //  @Injection(name = "OAUTH_REDIRECT_URI")
  //  private String oauthRedirectUri;
  //
  //  /** OAuth Access Token */
  //  @Injection(name = "OAUTH_ACCESS_TOKEN")
  //  private String oauthAccessToken;
  //
  //  /** OAuth Refresh Token */
  //  @Injection(name = "OAUTH_REFRESH_TOKEN")
  //  private String oauthRefreshToken;
  //
  //  /** OAuth Instance URL */
  //  @Injection(name = "OAUTH_INSTANCE_URL")
  //  private String oauthInstanceUrl;

  /** The password */
  @HopMetadataProperty(key = "password", injectionKey = "SALESFORCE_PASSWOR", password = true)
  //  @Injection(name = "SALESFORCE_PASSWORD")
  private String password;

  /** The time out */
  @HopMetadataProperty(key = "timeout", injectionKey = "TIME_OUT")
  //  @Injection(name = "TIME_OUT")
  private String timeout;

  /** The connection compression */
  @HopMetadataProperty(key = "useCompression", injectionKey = "USE_COMPRESSION")
  //  @Injection(name = "USE_COMPRESSION")
  private boolean compression;

  /** The Salesforce module */
  @HopMetadataProperty(key = "module", injectionKey = "MODULE")
  //  @Injection(name = "MODULE")
  private String module;

  /** Salesforce Connection metadata name */
  @HopMetadataProperty(key = "salesforce_connection", injectionKey = "SALESFORCE_CONNECTION")
  private String salesforceConnection;

  /**
   * @return Returns the Salesforce Connection metadata name.
   */
  public String getSalesforceConnection() {
    return salesforceConnection;
  }

  /**
   * @param salesforceConnection The Salesforce Connection metadata name to set.
   */
  public void setSalesforceConnection(String salesforceConnection) {
    this.salesforceConnection = salesforceConnection;
  }

  //  @Override
  //  public String getXml() {
  //    StringBuilder retval = new StringBuilder();
  //    retval.append("    ").append(XmlHandler.addTagValue("targeturl", getTargetUrl()));
  //    retval.append("    ").append(XmlHandler.addTagValue("username", getUsername()));
  //    retval
  //        .append("    ")
  //        .append(
  //            XmlHandler.addTagValue(
  //                "password", Encr.encryptPasswordIfNotUsingVariables(getPassword())));
  //    retval.append("    ").append(XmlHandler.addTagValue("timeout", getTimeout()));
  //    retval.append("    ").append(XmlHandler.addTagValue("useCompression", isCompression()));
  //    retval.append("    ").append(XmlHandler.addTagValue("module", getModule()));
  //    return retval.toString();
  //  }
  //
  //  @Override
  //  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
  //      throws HopXmlException {
  //    setTargetUrl(XmlHandler.getTagValue(transformNode, "targeturl"));
  //    setUsername(XmlHandler.getTagValue(transformNode, "username"));
  //    setPassword(
  //        Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(transformNode,
  // "password")));
  //    setTimeout(XmlHandler.getTagValue(transformNode, "timeout"));
  //    setCompression("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode,
  // "useCompression")));
  //    setModule(XmlHandler.getTagValue(transformNode, "module"));
  //  }

  @Override
  public Object clone() {
    SalesforceTransformMeta retval = (SalesforceTransformMeta) super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    setTargetUrl(SalesforceConnectionUtils.TARGET_DEFAULT_URL);
    setUsername("");
    setPassword("");
    setTimeout("60000");
    setCompression(false);
    setModule("Account");
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    // check URL
    if (Utils.isEmpty(getTargetUrl())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.NoURL"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.URLOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check user name
    if (Utils.isEmpty(getUsername())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.NoUsername"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.UsernameOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check module
    if (Utils.isEmpty(getModule())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.NoModule"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.ModuleOk"),
              transformMeta);
    }
    remarks.add(cr);
  }

  /**
   * @return Returns the Target URL.
   */
  //  public String getTargetUrl() {
  //    return targetUrl;
  //  }

  /**
   * @param targetUrl The Target URL to set.
   */
  //  public void setTargetUrl(String targetUrl) {
  //    this.targetUrl = targetUrl;
  //  }

  /**
   * @return Returns the UserName.
   */
  //  public String getUsername() {
  //    return username;
  //  }

  /**
   * @param username The Username to set.
   */
  //  public void setUsername(String username) {
  //    this.username = username;
  //  }

  /**
   * @return Returns the Password.
   */
  //  public String getPassword() {
  //    return password;
  //  }

  /**
   * @param password The password to set.
   */
  //  public void setPassword(String password) {
  //    this.password = password;
  //  }

  /**
   * @return Returns the Authentication Type.
   */
  //  public String getAuthenticationType() {
  //    return authenticationType;
  //  }

  /**
   * @param authenticationType The Authentication Type to set.
   */
  //  public void setAuthenticationType(String authenticationType) {
  //    this.authenticationType = authenticationType;
  //  }

  /**
   * @return Returns the OAuth Client ID.
   */
  //  public String getOauthClientId() {
  //    return oauthClientId;
  //  }

  /**
   * @param oauthClientId The OAuth Client ID to set.
   */
  //  public void setOauthClientId(String oauthClientId) {
  //    this.oauthClientId = oauthClientId;
  //  }

  /**
   * @return Returns the OAuth Client Secret.
   */
  //  public String getOauthClientSecret() {
  //    return oauthClientSecret;
  //  }

  /**
   * @param oauthClientSecret The OAuth Client Secret to set.
   */
  //  public void setOauthClientSecret(String oauthClientSecret) {
  //    this.oauthClientSecret = oauthClientSecret;
  //  }

  /**
   * @return Returns the OAuth Redirect URI.
   */
  //  public String getOauthRedirectUri() {
  //    return oauthRedirectUri;
  //  }

  /**
   * @param oauthRedirectUri The OAuth Redirect URI to set.
   */
  //  public void setOauthRedirectUri(String oauthRedirectUri) {
  //    this.oauthRedirectUri = oauthRedirectUri;
  //  }

  /**
   * @return Returns the OAuth Access Token.
   */
  //  public String getOauthAccessToken() {
  //    return oauthAccessToken;
  //  }

  /**
   * @param oauthAccessToken The OAuth Access Token to set.
   */
  //  public void setOauthAccessToken(String oauthAccessToken) {
  //    this.oauthAccessToken = oauthAccessToken;
  //  }

  /**
   * @return Returns the OAuth Refresh Token.
   */
  //  public String getOauthRefreshToken() {
  //    return oauthRefreshToken;
  //  }

  /**
   * @param oauthRefreshToken The OAuth Refresh Token to set.
   */
  //  public void setOauthRefreshToken(String oauthRefreshToken) {
  //    this.oauthRefreshToken = oauthRefreshToken;
  //  }

  /**
   * @return Returns the OAuth Instance URL.
   */
  //  public String getOauthInstanceUrl() {
  //    return oauthInstanceUrl;
  //  }

  /**
   * @param oauthInstanceUrl The OAuth Instance URL to set.
   */
  //  public void setOauthInstanceUrl(String oauthInstanceUrl) {
  //    this.oauthInstanceUrl = oauthInstanceUrl;
  //  }

  /**
   * @return Returns true if OAuth authentication is selected.
   */
  //  public boolean isOAuthAuthentication() {
  //    return "OAUTH".equalsIgnoreCase(authenticationType);
  //  }

  /**
   * @return Returns the connection timeout.
   */
  //  public String getTimeout() {
  //    return timeout;
  //  }

  /**
   * @param timeout The connection timeout to set.
   */
  //  public void setTimeout(String timeout) {
  //    this.timeout = timeout;
  //  }

  //  public boolean isCompression() {
  //    return compression;
  //  }

  //  public void setCompression(boolean compression) {
  //    this.compression = compression;
  //  }

  //  public String getModule() {
  //    return module;
  //  }

  //  public void setModule(String module) {
  //    this.module = module;
  //  }
}
