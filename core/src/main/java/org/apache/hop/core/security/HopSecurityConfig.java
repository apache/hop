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

package org.apache.hop.core.security;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Instance-wide security configuration for Hop Web (and future shared surfaces). Stored under
 * {@code HOP_CONFIG_FOLDER/security/security-config.json}.
 *
 * <p>Mode {@code NONE} (default) leaves sessions unrestricted. Mode {@code EXTERNAL} trusts the
 * servlet container principal and maps container roles to Hop roles. Mode {@code BASIC} enables
 * Hop-managed form login (styled Hop Web login page) via {@code HopBasicAuthFilter} and {@link
 * HopUserStore}. Mode {@code OAUTH2} enables OpenID Connect (authorization code + PKCE) via {@code
 * HopOidcAuthFilter}.
 *
 * <p>Environment override: {@code HOP_WEB_SECURITY_MODE} and OAuth env vars (see {@link
 * HopSecurityBootstrap}).
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class HopSecurityConfig {

  public static final String SECURITY_FOLDER = "security";
  public static final String SECURITY_CONFIG_FILENAME = "security-config.json";

  /** Authentication mode. */
  public enum AuthMode {
    NONE,
    EXTERNAL,
    BASIC,
    OAUTH2;

    public static AuthMode fromString(String value) {
      if (value == null || value.isBlank()) {
        return NONE;
      }
      try {
        return AuthMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        return NONE;
      }
    }
  }

  private String mode = AuthMode.NONE.name();

  /**
   * Optional explicit mapping from container role name → Hop role id ({@code admin}, {@code user},
   * {@code operator}, {@code readonly}). When empty, built-in aliases in {@link HopRole} are used.
   */
  private Map<String, String> roleMappings = new LinkedHashMap<>();

  /**
   * Custom welcome text shown on the Hop Web login page (BASIC / OAUTH2). Empty uses the built-in
   * default message.
   */
  private String welcomeMessage = "";

  public static final String DEFAULT_WELCOME_MESSAGE =
      "Sign in to design and run data pipelines and workflows.";

  // --- OAuth2 / OIDC (mode OAUTH2) ---

  /** OIDC issuer URL (e.g. https://keycloak.example/realms/hop). */
  private String oauthIssuerUrl = "";

  /** OAuth2 client id. */
  private String oauthClientId = "";

  /**
   * Optional client secret. Leave empty for public clients that use PKCE only.
   *
   * <p>Prefer {@code HOP_WEB_OAUTH_CLIENT_SECRET} env for production secrets.
   */
  private String oauthClientSecret = "";

  /**
   * Redirect URI registered with the IdP. Empty = auto {@code {origin}{context}/oauth/callback}.
   */
  private String oauthRedirectUri = "";

  /** Space-separated scopes (default openid profile email). */
  private String oauthScopes = "openid profile email";

  /**
   * Claim holding role/group names for mapping to Hop roles. Supports simple dotted paths such as
   * {@code groups}, {@code roles}, or {@code realm_access.roles} (Keycloak).
   */
  private String oauthRoleClaim = "groups";

  /** Claim used as Hop username (default {@code preferred_username}). */
  private String oauthUsernameClaim = "preferred_username";

  /** Use PKCE (S256) for the authorization code flow. Recommended true. */
  private boolean oauthUsePkce = true;

  /**
   * Optional end-session (RP-initiated logout) URL override. Empty uses discovery {@code
   * end_session_endpoint}.
   */
  private String oauthEndSessionEndpoint = "";

  private static volatile HopSecurityConfig cached;

  /** Effective client secret: env {@code HOP_WEB_OAUTH_CLIENT_SECRET} overrides config file. */
  public String resolveOauthClientSecret() {
    String env = System.getenv("HOP_WEB_OAUTH_CLIENT_SECRET");
    if (env != null && !env.isBlank()) {
      return env.trim();
    }
    String prop = System.getProperty("HOP_WEB_OAUTH_CLIENT_SECRET");
    if (prop != null && !prop.isBlank()) {
      return prop.trim();
    }
    return oauthClientSecret == null ? "" : oauthClientSecret;
  }

  public boolean isOauthConfigured() {
    return oauthIssuerUrl != null
        && !oauthIssuerUrl.isBlank()
        && oauthClientId != null
        && !oauthClientId.isBlank();
  }

  /**
   * Welcome message for the login page, never blank.
   *
   * @return configured message or {@link #DEFAULT_WELCOME_MESSAGE}
   */
  public String resolveWelcomeMessage() {
    if (welcomeMessage == null || welcomeMessage.isBlank()) {
      return DEFAULT_WELCOME_MESSAGE;
    }
    return welcomeMessage.trim();
  }

  public AuthMode getAuthMode() {
    return AuthMode.fromString(mode);
  }

  public void setAuthMode(AuthMode authMode) {
    this.mode = authMode != null ? authMode.name() : AuthMode.NONE.name();
  }

  /**
   * Path to the security config file (VFS-friendly absolute path string).
   *
   * @return config file path
   */
  public static String getConfigFilePath() {
    return Const.HOP_CONFIG_FOLDER
        + Const.FILE_SEPARATOR
        + SECURITY_FOLDER
        + Const.FILE_SEPARATOR
        + SECURITY_CONFIG_FILENAME;
  }

  /**
   * Load config from disk (cached). Missing file yields defaults (mode NONE).
   *
   * @return config instance (never null)
   */
  public static HopSecurityConfig load() {
    HopSecurityConfig local = cached;
    if (local != null) {
      return local;
    }
    synchronized (HopSecurityConfig.class) {
      if (cached != null) {
        return cached;
      }
      cached = readFromFile();
      return cached;
    }
  }

  /** Drop cached config so the next {@link #load()} re-reads from disk. */
  public static void clearCache() {
    cached = null;
  }

  /**
   * Persist config and update cache.
   *
   * @param config config to save
   */
  public static void save(HopSecurityConfig config) {
    if (config == null) {
      return;
    }
    writeToFile(config);
    cached = config;
  }

  private static HopSecurityConfig readFromFile() {
    String path = getConfigFilePath();
    try {
      if (!HopVfs.fileExists(path)) {
        return new HopSecurityConfig();
      }
      try (InputStream in = HopVfs.getInputStream(path)) {
        ObjectMapper mapper = HopJson.newMapper();
        HopSecurityConfig config = mapper.readValue(in, HopSecurityConfig.class);
        if (config.getRoleMappings() == null) {
          config.setRoleMappings(new LinkedHashMap<>());
        }
        return config;
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Unable to read Hop security config from '" + path + "', using defaults", e);
      return new HopSecurityConfig();
    }
  }

  private static void writeToFile(HopSecurityConfig config) {
    String path = getConfigFilePath();
    try {
      String folder = Const.HOP_CONFIG_FOLDER + Const.FILE_SEPARATOR + SECURITY_FOLDER;
      var folderObject = HopVfs.getFileObject(folder);
      if (!folderObject.exists()) {
        folderObject.createFolder();
      }
      ObjectMapper mapper = HopJson.newMapper();
      mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
      byte[] json = mapper.writeValueAsString(config).getBytes(StandardCharsets.UTF_8);
      try (OutputStream out = HopVfs.getOutputStream(path, false)) {
        out.write(json);
      }
      LogChannel.GENERAL.logBasic("Saved Hop security config to '" + path + "'");
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Unable to save Hop security config to '" + path + "'", e);
    }
  }

  /**
   * Resolve a container role name to a Hop role using configured mappings first, then built-in
   * aliases.
   *
   * @param containerRole container / IdP role name
   * @return Hop role or null
   */
  public HopRole mapContainerRole(String containerRole) {
    if (containerRole == null || containerRole.isBlank()) {
      return null;
    }
    if (roleMappings != null && !roleMappings.isEmpty()) {
      String mapped = roleMappings.get(containerRole);
      if (mapped == null) {
        // case-insensitive key match
        for (Map.Entry<String, String> entry : roleMappings.entrySet()) {
          if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(containerRole)) {
            mapped = entry.getValue();
            break;
          }
        }
      }
      if (mapped != null) {
        HopRole role = HopRole.fromIdOrAlias(mapped);
        if (role != null) {
          return role;
        }
      }
    }
    return HopRole.fromIdOrAlias(containerRole);
  }
}
