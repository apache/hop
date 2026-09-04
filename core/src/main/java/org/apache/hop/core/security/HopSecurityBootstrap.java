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

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.oidc.HopOidcClient;

/**
 * One-shot bootstrap for Hop Web security: apply env overrides and ensure BASIC mode has at least
 * one admin user.
 *
 * <p>Environment variables:
 *
 * <ul>
 *   <li>{@code HOP_WEB_SECURITY_MODE} — {@code NONE}, {@code EXTERNAL}, {@code BASIC}, {@code
 *       OAUTH2}
 *   <li>{@code HOP_WEB_ADMIN_USER} / {@code HOP_WEB_ADMIN_PASSWORD} — first admin when user store
 *       is empty under BASIC
 *   <li>{@code HOP_WEB_ALLOW_DEFAULT_ADMIN} — {@code true} to allow default {@code admin}/{@code
 *       admin} bootstrap (local only)
 *   <li>{@code HOP_WEB_SEED_DEMO_USERS} — seed demo BASIC users
 *   <li>{@code HOP_WEB_OAUTH_ISSUER}, {@code HOP_WEB_OAUTH_CLIENT_ID}, {@code
 *       HOP_WEB_OAUTH_CLIENT_SECRET}, {@code HOP_WEB_OAUTH_REDIRECT_URI}, {@code
 *       HOP_WEB_OAUTH_SCOPES}, {@code HOP_WEB_OAUTH_ROLE_CLAIM}, {@code
 *       HOP_WEB_OAUTH_USERNAME_CLAIM}
 * </ul>
 */
public final class HopSecurityBootstrap {

  public static final String ENV_SECURITY_MODE = "HOP_WEB_SECURITY_MODE";
  public static final String ENV_ALLOW_DEFAULT_ADMIN = "HOP_WEB_ALLOW_DEFAULT_ADMIN";
  public static final String ENV_SEED_DEMO_USERS = "HOP_WEB_SEED_DEMO_USERS";
  public static final String ENV_ALLOW_UNAUTHENTICATED_SERVER_API =
      "HOP_WEB_ALLOW_UNAUTHENTICATED_SERVER_API";

  public static final String ENV_OAUTH_ISSUER = "HOP_WEB_OAUTH_ISSUER";
  public static final String ENV_OAUTH_CLIENT_ID = "HOP_WEB_OAUTH_CLIENT_ID";
  public static final String ENV_OAUTH_CLIENT_SECRET = "HOP_WEB_OAUTH_CLIENT_SECRET";
  public static final String ENV_OAUTH_REDIRECT_URI = "HOP_WEB_OAUTH_REDIRECT_URI";
  public static final String ENV_OAUTH_SCOPES = "HOP_WEB_OAUTH_SCOPES";
  public static final String ENV_OAUTH_ROLE_CLAIM = "HOP_WEB_OAUTH_ROLE_CLAIM";
  public static final String ENV_OAUTH_USERNAME_CLAIM = "HOP_WEB_OAUTH_USERNAME_CLAIM";

  private static volatile boolean ran;

  private HopSecurityBootstrap() {}

  /** Run bootstrap once per JVM. Safe to call repeatedly. */
  public static synchronized void runOnce() {
    if (ran) {
      return;
    }
    ran = true;
    try {
      HopUserStore.applyEnvironmentModeOverride();
      applyOauthEnvironmentOverrides();
      applyServerApiEnvironmentOverride();
      HopSecurityConfig.clearCache();
      HopOidcClient.clearDiscoveryCache();
      HopSecurityConfig config = HopSecurityConfig.load();
      if (config.getAuthMode() == HopSecurityConfig.AuthMode.BASIC) {
        boolean allowDefault =
            isTruthy(System.getenv(ENV_ALLOW_DEFAULT_ADMIN))
                || isTruthy(System.getProperty(ENV_ALLOW_DEFAULT_ADMIN));
        boolean seedDemo =
            isTruthy(System.getenv(ENV_SEED_DEMO_USERS))
                || isTruthy(System.getProperty(ENV_SEED_DEMO_USERS));
        HopUserStore store = HopUserStore.getInstance();
        if (seedDemo) {
          store.seedDemoUsersIfEmpty();
        } else {
          store.bootstrapAdminIfEmpty(allowDefault);
        }
        if (store.isEmpty()) {
          LogChannel.GENERAL.logError(
              "Hop BASIC auth is enabled but the user store is empty. "
                  + "Set "
                  + HopUserStore.ENV_ADMIN_USER
                  + " and "
                  + HopUserStore.ENV_ADMIN_PASSWORD
                  + ", or "
                  + ENV_ALLOW_DEFAULT_ADMIN
                  + "=true / "
                  + ENV_SEED_DEMO_USERS
                  + "=true for local development.");
        } else {
          LogChannel.GENERAL.logBasic(
              "Hop BASIC auth enabled with {0} user(s)", store.listUsers().size());
        }
      } else if (config.getAuthMode() == HopSecurityConfig.AuthMode.OAUTH2) {
        if (!config.isOauthConfigured()) {
          LogChannel.GENERAL.logError(
              "Hop OAUTH2 mode is enabled but issuer/client id are not set. "
                  + "Configure Configuration → Security or set "
                  + ENV_OAUTH_ISSUER
                  + " and "
                  + ENV_OAUTH_CLIENT_ID
                  + ".");
        } else {
          LogChannel.GENERAL.logBasic(
              "Hop OAUTH2 / OIDC enabled for issuer ''{0}''", config.getOauthIssuerUrl());
        }
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Hop security bootstrap failed", e);
    }
  }

  /**
   * Apply {@link #ENV_ALLOW_UNAUTHENTICATED_SERVER_API} into security-config.json when present.
   * Only governs mode {@code NONE}; the authenticated modes always enforce RBAC on {@code /hop/*}.
   */
  public static void applyServerApiEnvironmentOverride() {
    String value = env(ENV_ALLOW_UNAUTHENTICATED_SERVER_API);
    if (value == null) {
      return;
    }
    HopSecurityConfig config = HopSecurityConfig.load();
    boolean allow = isTruthy(value);
    if (config.isAllowUnauthenticatedServerApi() != allow) {
      config.setAllowUnauthenticatedServerApi(allow);
      HopSecurityConfig.save(config);
      LogChannel.GENERAL.logBasic(
          "Hop Server API in mode NONE set to "
              + (allow ? "OPEN" : "CLOSED")
              + " from "
              + ENV_ALLOW_UNAUTHENTICATED_SERVER_API);
    }
  }

  /** Apply OAuth-related env vars into security-config.json when present. */
  public static void applyOauthEnvironmentOverrides() {
    HopSecurityConfig config = HopSecurityConfig.load();
    boolean changed = false;
    String issuer = env(ENV_OAUTH_ISSUER);
    if (issuer != null) {
      config.setOauthIssuerUrl(issuer);
      changed = true;
    }
    String clientId = env(ENV_OAUTH_CLIENT_ID);
    if (clientId != null) {
      config.setOauthClientId(clientId);
      changed = true;
    }
    // Secret preferably only via env at runtime (resolveOauthClientSecret); optionally persist
    String secret = env(ENV_OAUTH_CLIENT_SECRET);
    if (secret != null) {
      config.setOauthClientSecret(secret);
      changed = true;
    }
    String redirect = env(ENV_OAUTH_REDIRECT_URI);
    if (redirect != null) {
      config.setOauthRedirectUri(redirect);
      changed = true;
    }
    String scopes = env(ENV_OAUTH_SCOPES);
    if (scopes != null) {
      config.setOauthScopes(scopes);
      changed = true;
    }
    String roleClaim = env(ENV_OAUTH_ROLE_CLAIM);
    if (roleClaim != null) {
      config.setOauthRoleClaim(roleClaim);
      changed = true;
    }
    String userClaim = env(ENV_OAUTH_USERNAME_CLAIM);
    if (userClaim != null) {
      config.setOauthUsernameClaim(userClaim);
      changed = true;
    }
    if (changed) {
      HopSecurityConfig.save(config);
      LogChannel.GENERAL.logBasic("Applied OAuth environment overrides to security config");
    }
  }

  /** Reset for tests. */
  public static synchronized void reset() {
    ran = false;
  }

  private static String env(String name) {
    String v = System.getenv(name);
    if (v != null && !v.isBlank()) {
      return v.trim();
    }
    v = System.getProperty(name);
    if (v != null && !v.isBlank()) {
      return v.trim();
    }
    return null;
  }

  private static boolean isTruthy(String value) {
    if (value == null) {
      return false;
    }
    String v = value.trim().toLowerCase();
    return "true".equals(v) || "yes".equals(v) || "y".equals(v) || "1".equals(v);
  }
}
