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

package org.apache.hop.ui.hopgui.security;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.hop.core.security.HopSecurityConfig;

/** Renders the Hop Web login HTML page (form-based BASIC mode). */
public final class HopLoginPage {

  public static final String PATH_LOGIN = "/login";
  public static final String PATH_LOGIN_CSS = "/login/login.css";
  public static final String PATH_LOGIN_LOGO = "/login/logo.svg";

  private HopLoginPage() {}

  /**
   * Build the login page HTML.
   *
   * @param contextPath servlet context path (may be empty or start with /)
   * @param welcomeMessage message under the title
   * @param errorMessage optional error (null if none)
   * @param redirectPath relative path after login (e.g. /ui)
   * @param usernamePrefill optional username to re-fill after error
   * @return HTML document
   */
  public static String render(
      String contextPath,
      String welcomeMessage,
      String errorMessage,
      String redirectPath,
      String usernamePrefill) {
    return renderInternal(
        contextPath, welcomeMessage, errorMessage, redirectPath, usernamePrefill, false, true);
  }

  /**
   * OAuth2 / OIDC login page with SSO button.
   *
   * @param oauthConfigured whether issuer + client id are set
   */
  public static String renderOauth(
      String contextPath,
      String welcomeMessage,
      String errorMessage,
      String redirectPath,
      boolean oauthConfigured) {
    return renderInternal(
        contextPath, welcomeMessage, errorMessage, redirectPath, null, true, oauthConfigured);
  }

  private static String renderInternal(
      String contextPath,
      String welcomeMessage,
      String errorMessage,
      String redirectPath,
      String usernamePrefill,
      boolean oauthMode,
      boolean oauthConfigured) {

    String ctx = normalizeContext(contextPath);
    String welcome =
        escapeHtml(
            welcomeMessage == null || welcomeMessage.isBlank()
                ? HopSecurityConfig.DEFAULT_WELCOME_MESSAGE
                : welcomeMessage);
    String redirect = escapeHtml(sanitizeRedirect(redirectPath, ctx));
    String user = escapeHtml(usernamePrefill == null ? "" : usernamePrefill);
    String cssHref = escapeHtml(ctx + PATH_LOGIN_CSS);
    String logoHref = escapeHtml(ctx + PATH_LOGIN_LOGO);
    String action = escapeHtml(ctx + PATH_LOGIN);
    String ssoHref =
        escapeHtml(ctx + "/oauth/start?redirect=" + urlEncode(sanitizeRedirect(redirectPath, ctx)));

    StringBuilder errorBlock = new StringBuilder();
    if (errorMessage != null && !errorMessage.isBlank()) {
      errorBlock
          .append("<div class=\"error\" role=\"alert\">")
          .append(escapeHtml(errorMessage))
          .append("</div>");
    }

    String formBlock;
    if (oauthMode) {
      if (oauthConfigured) {
        formBlock =
            """
            <div class="actions">
              <a class="btn-primary" style="display:block;text-align:center;text-decoration:none"
                 href="%s">Continue with SSO</a>
            </div>
            <p class="footer" style="margin-top:16px">You will be redirected to your identity provider.</p>
            """
                .formatted(ssoHref);
      } else {
        formBlock =
            """
            <div class="error" role="alert">
              OAuth2 / OIDC is not configured yet. An administrator must set the issuer URL and client ID
              (Configuration → Security), or set HOP_WEB_OAUTH_ISSUER and HOP_WEB_OAUTH_CLIENT_ID.
            </div>
            """;
      }
    } else {
      formBlock =
          """
          <form method="post" action="%s" autocomplete="on">
            <input type="hidden" name="redirect" value="%s"/>
            <div class="field">
              <label for="username">Username</label>
              <input id="username" name="username" type="text" required autofocus
                     autocomplete="username" value="%s" placeholder="Enter your username"/>
            </div>
            <div class="field">
              <label for="password">Password</label>
              <input id="password" name="password" type="password" required
                     autocomplete="current-password" placeholder="Enter your password"/>
            </div>
            <div class="actions">
              <button class="btn-primary" type="submit">Sign in</button>
            </div>
          </form>
          """
              .formatted(action, redirect, user);
    }

    String badge = oauthMode ? "Single sign-on" : "Secure sign-in";
    String focusScript =
        oauthMode
            ? ""
            : "document.getElementById('username') && document.getElementById('username').focus();";

    return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8"/>
          <meta name="viewport" content="width=device-width, initial-scale=1"/>
          <meta name="robots" content="noindex,nofollow"/>
          <title>Sign in · Apache Hop Web</title>
          <link rel="stylesheet" href="%s"/>
          <link rel="icon" href="%s" type="image/svg+xml"/>
        </head>
        <body>
          <div class="shell">
            <div class="card">
              <header class="brand">
                <img class="brand-logo" src="%s" width="72" height="72" alt="Apache Hop"/>
                <h1>Apache Hop Web</h1>
                <p class="subtitle">Data orchestration in the browser</p>
                <span class="badge">%s</span>
              </header>
              <div class="panel">
                <p class="welcome">%s</p>
                %s
                %s
                <p class="footer">
                  Powered by
                  <a href="https://hop.apache.org/" target="_blank" rel="noopener">Apache Hop</a>
                </p>
              </div>
            </div>
          </div>
          <script>%s</script>
        </body>
        </html>
        """
        .formatted(cssHref, logoHref, logoHref, badge, welcome, errorBlock, formBlock, focusScript);
  }

  private static String urlEncode(String value) {
    return java.net.URLEncoder.encode(
        value == null ? "" : value, java.nio.charset.StandardCharsets.UTF_8);
  }

  public static String loadCss() throws IOException {
    return loadClasspathResource(
        "org/apache/hop/ui/hopgui/login/login.css", HopLoginPage.class.getClassLoader());
  }

  public static byte[] loadLogoSvg() throws IOException {
    ClassLoader cl = HopLoginPage.class.getClassLoader();
    for (String path : new String[] {"ui/images/logo_hop.svg", "ui/images/logo_icon.svg"}) {
      try (InputStream in = cl.getResourceAsStream(path)) {
        if (in != null) {
          return in.readAllBytes();
        }
      }
    }
    // Minimal fallback mark using Hop brand colors
    String fallback =
        """
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 128 128">
          <rect width="128" height="128" rx="24" fill="#033d5d"/>
          <text x="64" y="78" text-anchor="middle" font-family="system-ui,sans-serif"
                font-size="48" font-weight="700" fill="#b9e5fa">H</text>
        </svg>
        """;
    return fallback.getBytes(StandardCharsets.UTF_8);
  }

  private static String loadClasspathResource(String path, ClassLoader cl) throws IOException {
    try (InputStream in = cl.getResourceAsStream(path)) {
      if (in == null) {
        throw new IOException("Resource not found: " + path);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  public static String normalizeContext(String contextPath) {
    if (contextPath == null || contextPath.isBlank() || "/".equals(contextPath)) {
      return "";
    }
    String c = contextPath.trim();
    if (c.endsWith("/")) {
      c = c.substring(0, c.length() - 1);
    }
    return c;
  }

  public static String sanitizeRedirect(String redirect, String contextPath) {
    String fallback = contextPath + "/ui";
    if (redirect == null || redirect.isBlank()) {
      return fallback;
    }
    String r = redirect.trim();
    // Block open redirects
    if (r.startsWith("http://") || r.startsWith("https://") || r.startsWith("//")) {
      return fallback;
    }
    if (!r.startsWith("/")) {
      r = "/" + r;
    }
    // Stay within this app
    if (!contextPath.isEmpty() && !r.startsWith(contextPath + "/") && !r.equals(contextPath)) {
      return fallback;
    }
    if (r.contains("..")) {
      return fallback;
    }
    return r;
  }

  static String escapeHtml(String raw) {
    if (raw == null) {
      return "";
    }
    return raw.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;");
  }

  public static boolean isLoginAssetPath(String path) {
    if (path == null) {
      return false;
    }
    return path.equals(PATH_LOGIN)
        || path.equals(PATH_LOGIN + "/")
        || path.equals(PATH_LOGIN_CSS)
        || path.equals(PATH_LOGIN_LOGO);
  }

  public static boolean isLoginPost(HttpMethod method, String path) {
    return Objects.equals(method, HttpMethod.POST)
        && (PATH_LOGIN.equals(path) || (PATH_LOGIN + "/").equals(path));
  }

  public enum HttpMethod {
    GET,
    POST,
    OTHER;

    public static HttpMethod of(String method) {
      if (method == null) {
        return OTHER;
      }
      return switch (method.toUpperCase()) {
        case "GET", "HEAD" -> GET;
        case "POST" -> POST;
        default -> OTHER;
      };
    }
  }
}
