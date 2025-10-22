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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "salesforceconnection",
    name = "i18n::SalesforceConnection.name",
    description = "i18n::SalesforceConnection.description",
    image = "salesforce.svg",
    documentationUrl = "/metadata-types/salesforce-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.REST_CONNECTION)
@Getter
@Setter
public class SalesforceConnection extends HopMetadataBase implements IHopMetadata {

  // Authentication Type
  @HopMetadataProperty(key = "authentication_type", injectionKey = "AUTHENTICATION_TYPE")
  private String authenticationType = "USERNAME_PASSWORD";

  // Username/Password Authentication
  @HopMetadataProperty(key = "username", injectionKey = "USERNAME")
  private String username;

  @HopMetadataProperty(key = "password", injectionKey = "PASSWORD")
  private String password;

  @HopMetadataProperty(key = "security_token", injectionKey = "SECURITY_TOKEN")
  private String securityToken;

  @HopMetadataProperty(key = "target_url", injectionKey = "TARGET_URL")
  private String targetUrl;

  // OAuth Authentication
  @HopMetadataProperty(key = "oauth_client_id", injectionKey = "OAUTH_CLIENT_ID")
  private String oauthClientId;

  @HopMetadataProperty(key = "oauth_client_secret", injectionKey = "OAUTH_CLIENT_SECRET")
  private String oauthClientSecret;

  @HopMetadataProperty(key = "oauth_redirect_uri", injectionKey = "OAUTH_REDIRECT_URI")
  private String oauthRedirectUri = "http://localhost:8080/callback";

  @HopMetadataProperty(
      key = "oauth_access_token",
      injectionKey = "OAUTH_ACCESS_TOKEN",
      password = true)
  private String oauthAccessToken;

  @HopMetadataProperty(
      key = "oauth_refresh_token",
      injectionKey = "OAUTH_REFRESH_TOKEN",
      password = true)
  private String oauthRefreshToken;

  @HopMetadataProperty(key = "oauth_instance_url", injectionKey = "OAUTH_INSTANCE_URL")
  private String oauthInstanceUrl;

  public SalesforceConnection() {
    super();
  }

  public SalesforceConnection(String name) {
    super(name);
  }

  /**
   * Check if OAuth authentication is selected
   *
   * @return true if OAuth authentication is selected
   */
  public boolean isOAuthAuthentication() {
    return "OAUTH".equalsIgnoreCase(authenticationType);
  }

  /**
   * Check if username/password authentication is selected
   *
   * @return true if username/password authentication is selected
   */
  public boolean isUsernamePasswordAuthentication() {
    return "USERNAME_PASSWORD".equalsIgnoreCase(authenticationType);
  }

  /**
   * Create a Salesforce connection that can be used by transforms
   *
   * @param variables Variables for resolving values
   * @param log Log channel for logging
   * @return A configured SalesforceConnection ready to use
   * @throws HopException if connection creation fails
   */
  public org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection createConnection(
      IVariables variables, org.apache.hop.core.logging.ILogChannel log) throws HopException {
    if (isOAuthAuthentication()) {
      // Resolve OAuth variables
      String clientId = variables.resolve(this.oauthClientId);
      String clientSecret =
          org.apache.hop.core.util.Utils.resolvePassword(variables, this.oauthClientSecret);
      String accessToken =
          org.apache.hop.core.util.Utils.resolvePassword(variables, this.oauthAccessToken);
      String refreshToken =
          org.apache.hop.core.util.Utils.resolvePassword(variables, this.oauthRefreshToken);
      String instanceUrl = variables.resolve(this.oauthInstanceUrl);

      org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection connection =
          new org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection(
              log, clientId, clientSecret, accessToken, instanceUrl);

      // Set the refresh token for token refresh functionality
      if (!org.apache.hop.core.util.Utils.isEmpty(refreshToken)) {
        connection.setOauthRefreshToken(refreshToken);
      }

      return connection;
    } else {
      // Resolve username/password variables
      String url = variables.resolve(this.targetUrl);
      String user = variables.resolve(this.username);
      String pass = org.apache.hop.core.util.Utils.resolvePassword(variables, this.password);

      // Combine password with security token if present
      String secToken = variables.resolve(this.securityToken);
      if (!org.apache.hop.core.util.Utils.isEmpty(secToken)) {
        pass = pass + secToken;
      }

      return new org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection(
          log, url, user, pass);
    }
  }

  /**
   * Test the connection by attempting to connect to Salesforce
   *
   * @param variables Variables for resolving values
   * @param log Log channel for logging
   * @throws HopException if connection test fails
   */
  public void testConnection(IVariables variables, org.apache.hop.core.logging.ILogChannel log)
      throws HopException {
    org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection sfConnection = null;
    try {
      // Create and connect
      sfConnection = createConnection(variables, log);
      sfConnection.connect();

      // Connection successful
      if (log.isDetailed()) {
        log.logDetailed("Salesforce connection test successful");
      }

    } finally {
      if (sfConnection != null) {
        try {
          sfConnection.close();
        } catch (Exception e) {
          // Ignore close errors
        }
      }
    }
  }

  // Getter and Setter methods
  public String getAuthenticationType() {
    return authenticationType;
  }

  public void setAuthenticationType(String authenticationType) {
    this.authenticationType = authenticationType;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getSecurityToken() {
    return securityToken;
  }

  public void setSecurityToken(String securityToken) {
    this.securityToken = securityToken;
  }

  public String getTargetUrl() {
    return targetUrl;
  }

  public void setTargetUrl(String targetUrl) {
    this.targetUrl = targetUrl;
  }

  public String getOauthClientId() {
    return oauthClientId;
  }

  public void setOauthClientId(String oauthClientId) {
    this.oauthClientId = oauthClientId;
  }

  public String getOauthClientSecret() {
    return oauthClientSecret;
  }

  public void setOauthClientSecret(String oauthClientSecret) {
    this.oauthClientSecret = oauthClientSecret;
  }

  public String getOauthRedirectUri() {
    return oauthRedirectUri;
  }

  public void setOauthRedirectUri(String oauthRedirectUri) {
    this.oauthRedirectUri = oauthRedirectUri;
  }

  public String getOauthAccessToken() {
    return oauthAccessToken;
  }

  public void setOauthAccessToken(String oauthAccessToken) {
    this.oauthAccessToken = oauthAccessToken;
  }

  public String getOauthRefreshToken() {
    return oauthRefreshToken;
  }

  public void setOauthRefreshToken(String oauthRefreshToken) {
    this.oauthRefreshToken = oauthRefreshToken;
  }

  public String getOauthInstanceUrl() {
    return oauthInstanceUrl;
  }

  public void setOauthInstanceUrl(String oauthInstanceUrl) {
    this.oauthInstanceUrl = oauthInstanceUrl;
  }
}
