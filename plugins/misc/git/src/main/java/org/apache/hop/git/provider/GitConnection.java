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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@Getter
@Setter
@HopMetadata(
    key = "gitconnection",
    name = "i18n::GitConnection.name",
    description = "i18n::GitConnection.description",
    image = "git.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/git-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.GIT_CONNECTION,
    supportsGlobalReplace = true)
public class GitConnection extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty(key = "provider")
  private GitProvider provider = GitProvider.GITHUB_CLOUD;

  /**
   * Left null on a connection saved before auth types existed; {@link #getAuthType()} then falls
   * back to the provider default, which is what those connections already did.
   */
  @HopMetadataProperty(key = "auth_type")
  private GitAuthType authType;

  @HopMetadataProperty(key = "api_base_url")
  private String apiBaseUrl;

  @HopMetadataProperty(key = "username")
  private String username;

  @HopMetadataProperty(key = "token", password = true)
  private String token;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  public GitConnection() {
    super();
  }

  public GitConnection(GitConnection source) {
    super(source);
    this.provider = source.provider;
    this.authType = source.authType;
    this.apiBaseUrl = source.apiBaseUrl;
    this.username = source.username;
    this.token = source.token;
    this.password = source.password;
  }

  @Override
  public GitConnection clone() {
    return new GitConnection(this);
  }

  public GitProvider getGitProvider() {
    return provider == null ? GitProvider.GITHUB_CLOUD : provider;
  }

  public GitProvider getProvider() {
    return getGitProvider();
  }

  /** The stored auth type, or the provider default when the connection predates the setting. */
  public GitAuthType getAuthType() {
    GitProvider gitProvider = getGitProvider();
    if (authType == null || !gitProvider.supports(authType)) {
      return gitProvider.getDefaultAuthType();
    }
    return authType;
  }

  public void setProvider(GitProvider gitProvider) {
    this.provider = gitProvider == null ? GitProvider.GITHUB_CLOUD : gitProvider;
  }

  public void setGitProvider(GitProvider gitProvider) {
    setProvider(gitProvider);
  }

  public String getResolvedApiBaseUrl(IVariables variables) throws HopException {
    GitProvider gitProvider = getGitProvider();
    String resolved = apiBaseUrl;
    if (variables != null && apiBaseUrl != null) {
      resolved = variables.resolve(apiBaseUrl);
    }
    if (!Utils.isEmpty(resolved)) {
      return resolved.trim();
    }
    String defaultUrl = gitProvider.getDefaultApiBaseUrl();
    if (defaultUrl == null || defaultUrl.isBlank()) {
      // Unchecked would surface as a raw stack trace mid-pipeline instead of this sentence.
      throw new HopException(
          gitProvider.getDisplayName()
              + " has no default API URL because it is self-hosted. Set the API base URL on the"
              + " Git connection, for example https://git.example.com/api/v1.");
    }
    return defaultUrl;
  }

  public GitAuth toAuth() throws HopException {
    return toAuth(null);
  }

  public GitAuth toAuth(IVariables variables) throws HopException {
    GitProvider gitProvider = getGitProvider();
    GitAuthType type = getAuthType();
    GitProvider.AuthStyle style = gitProvider.getAuthStyle(type);
    if (type == GitAuthType.BASIC) {
      String user = resolveSecret(variables, username, "username");
      String pass = resolveSecret(variables, password, "password");
      return GitAuth.forBasic(style, user, pass);
    }
    String label = type == GitAuthType.OAUTH2 ? "OAuth 2 access token" : "personal access token";
    return GitAuth.forToken(type, style, resolveSecret(variables, token, label));
  }

  private static String resolveSecret(IVariables variables, String value, String label)
      throws HopException {
    if (value == null) {
      return null;
    }
    String resolved = variables != null ? variables.resolve(value) : value;
    String decrypted = Encr.decryptPasswordOptionallyEncrypted(resolved);
    if (decrypted != null) {
      decrypted = decrypted.trim();
    }
    if (Utils.isEmpty(decrypted)) {
      return null;
    }
    if (decrypted.contains("${") && decrypted.contains("}")) {
      // The value itself is a credential: naming it here would copy the secret into the log for
      // any password that legitimately contains ${ and }.
      throw new HopException(
          "The Git connection "
              + label
              + " still contains an unresolved variable. Set that variable in the Hop environment,"
              + " or enter the value directly.");
    }
    return decrypted;
  }

  /**
   * Checks that these settings can talk to the provider API.
   *
   * <p>A credential is not required. Reading a public repository works anonymously on every
   * provider except Bitbucket, so demanding a token here would refuse connections the Git Input
   * transform runs against perfectly well. What is verified therefore depends on what was given:
   * with a credential the organizations of the account are listed, which proves the credential
   * itself; without one the check falls back to a public read of the API.
   */
  public void test(IVariables variables) throws HopException {
    String apiBaseUrl = getResolvedApiBaseUrl(variables);
    GitAuth auth = toAuth(variables);
    if (getAuthType() == GitAuthType.BASIC && auth == null) {
      throw new HopException(
          getGitProvider().getDisplayName()
              + " basic authentication needs both a username and a password. An account that only"
              + " signs in through an external provider usually has no local password; use a"
              + " personal access token instead.");
    }
    GitRepositoryBrowser.forProvider(getGitProvider()).verify(apiBaseUrl, auth);
  }

  public String getDisplayApiBaseUrl() {
    if (!Utils.isEmpty(apiBaseUrl)) {
      return apiBaseUrl;
    }
    return getGitProvider().getUiDefaultApiBaseUrl();
  }

  public static String normalizeApiBaseUrlForStorage(String entered, GitProvider provider) {
    if (entered == null) {
      return "";
    }
    String trimmed = entered.trim();
    if (trimmed.isEmpty()) {
      return "";
    }
    String defaultUrl = provider.getDefaultApiBaseUrl();
    if (defaultUrl != null && trimmed.equals(defaultUrl)) {
      return "";
    }
    // The editor prefills the example URL as a hint. Storing it would leave a connection pointing
    // at example.com, which fails later with a DNS error rather than "you did not set a URL".
    String hint = provider.getApiBaseUrlHint();
    if (hint != null && trimmed.equals(hint)) {
      return "";
    }
    return entered;
  }
}
