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
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
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
   * Whether the embedded Hop Server API ({@code /hop/*}) is reachable in mode {@code NONE}.
   *
   * <p>In the authenticated modes ({@code BASIC}, {@code EXTERNAL}, {@code OAUTH2}) the API is
   * always available and governed by role-based access control. Mode {@code NONE} has no user
   * identity, so the API can only be all-open or all-closed: this flag decides which, and defaults
   * to {@code false} (closed) so the default open Hop Web install does not expose unauthenticated
   * pipeline and workflow execution. Set it to {@code true} (or {@code
   * HOP_WEB_ALLOW_UNAUTHENTICATED_SERVER_API}) to use Hop Web purely as an execution server behind
   * your own network controls.
   */
  private boolean allowUnauthenticatedServerApi = false;

  /**
   * Which browser requests the embedded Hop Server API ({@code /hop/*}) accepts, as a {@link
   * CrossSitePolicy} code.
   *
   * <p>The Hop Server servlets answer state-changing operations on {@code GET}, so without this a
   * page the operator happens to be visiting can drive them using the operator's own session. The
   * default {@code same-site} rejects requests started by another site; {@code same-origin} also
   * rejects other hosts of the same domain; {@code off} disables the check. Overridden by the
   * {@code HOP_SERVER_CROSS_SITE_POLICY} system property or environment variable, the same knob
   * hop-server uses.
   */
  private String crossSitePolicy = CrossSitePolicy.SAME_SITE.getCode();

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

  /** Last cross-site policy value warned about, so a bad one is not logged per request. */
  private static volatile String warnedCrossSitePolicy;

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

  /**
   * Effective cross-site policy, resolved as {@code -D} → environment → config file → {@link
   * CrossSitePolicy#SAME_SITE}, so a container can set it without editing {@code
   * security-config.json}.
   *
   * <p>A <em>blank</em> system property is treated as absent on purpose. {@code
   * HopEnvironment.init()} copies every {@code @Variable}-declared setting into a system property
   * and does not look at the environment, so the property is present but empty here unless an
   * operator really passed {@code -D}. Honouring a blank one would shadow both the environment
   * variable and the config file, which is exactly what it did before this was fixed.
   *
   * <p>An unreadable value falls back to the safe default rather than leaving the check off.
   *
   * @return the policy to apply, never null
   */
  public CrossSitePolicy resolveCrossSitePolicy() {
    String prop = System.getProperty(CrossSitePolicy.CONFIG_KEY);
    String env = System.getenv(CrossSitePolicy.CONFIG_KEY);
    String value = prop != null && !prop.isBlank() ? prop : env;
    if (value == null || value.isBlank()) {
      value = crossSitePolicy;
    }
    try {
      return CrossSitePolicy.parse(value);
    } catch (HopException e) {
      warnUnusableCrossSitePolicy(value);
      return CrossSitePolicy.SAME_SITE;
    }
  }

  /**
   * Complain about an unusable policy value once per distinct value.
   *
   * <p>Deliberately not {@code LogChannel}: this runs on every request through the Hop Web
   * cross-site filter, where the Hop log store is not guaranteed to be initialised and a throwing
   * logger would turn a configuration typo into a failed request. The de-duplication keeps a bad
   * value from filling the log one line per request.
   */
  private static void warnUnusableCrossSitePolicy(String value) {
    if (!Objects.equals(value, warnedCrossSitePolicy)) {
      warnedCrossSitePolicy = value;
      Logger.getLogger(HopSecurityConfig.class.getName())
          .log(
              Level.WARNING,
              "Unusable cross-site policy ''{0}'', falling back to ''{1}''",
              new Object[] {value, CrossSitePolicy.SAME_SITE.getCode()});
    }
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
