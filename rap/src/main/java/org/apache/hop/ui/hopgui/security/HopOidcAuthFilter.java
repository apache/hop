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

import com.nimbusds.jwt.JWTClaimsSet;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hop.core.security.HopSecurityBootstrap;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopUserStore;
import org.apache.hop.core.security.oidc.HopOidcClient;
import org.apache.hop.core.security.oidc.HopOidcClient.AuthSession;
import org.apache.hop.core.security.oidc.OidcTokenResponse;

/**
 * OpenID Connect (authorization code + PKCE) filter for mode {@link
 * HopSecurityConfig.AuthMode#OAUTH2}. No-op for other modes.
 *
 * <p>Paths:
 *
 * <ul>
 *   <li>{@code /login} — branded page with “Continue with SSO”
 *   <li>{@code /oauth/start} — redirects to the IdP
 *   <li>{@code /oauth/callback} — code exchange, session principal
 *   <li>{@code /oauth/logout} — clear session; optional RP-initiated logout
 * </ul>
 */
public class HopOidcAuthFilter implements Filter {

  private static final Logger LOG = Logger.getLogger(HopOidcAuthFilter.class.getName());

  public static final String PATH_OAUTH_START = "/oauth/start";
  public static final String PATH_OAUTH_CALLBACK = "/oauth/callback";
  public static final String PATH_OAUTH_LOGOUT = "/oauth/logout";

  public static final String SESSION_PRINCIPAL = HopBasicAuthFilter.SESSION_PRINCIPAL;
  public static final String SESSION_AUTH = "hop.oidc.auth.session";
  public static final String SESSION_ID_TOKEN = "hop.oidc.id_token";

  @Override
  public void init(FilterConfig filterConfig) {
    HopSecurityBootstrap.runOnce();
    LOG.info("HopOidcAuthFilter initialized (mode=" + HopSecurityConfig.load().getAuthMode() + ")");
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    if (!(request instanceof HttpServletRequest httpRequest)
        || !(response instanceof HttpServletResponse httpResponse)) {
      chain.doFilter(request, response);
      return;
    }

    // Re-read security-config.json so edits (role mappings, claims) apply without JVM restart
    HopSecurityConfig.clearCache();
    HopSecurityConfig config = HopSecurityConfig.load();
    if (config.getAuthMode() != HopSecurityConfig.AuthMode.OAUTH2) {
      chain.doFilter(request, response);
      return;
    }

    String contextPath = HopLoginPage.normalizeContext(httpRequest.getContextPath());
    String path = pathWithinApp(httpRequest, contextPath);

    // Public login assets (shared with BASIC)
    if (HopLoginPage.PATH_LOGIN_CSS.equals(path)) {
      serveCss(httpResponse);
      return;
    }
    if (HopLoginPage.PATH_LOGIN_LOGO.equals(path)) {
      serveLogo(httpResponse);
      return;
    }

    if (PATH_OAUTH_START.equals(path)) {
      handleStart(httpRequest, httpResponse, contextPath, config);
      return;
    }
    if (PATH_OAUTH_CALLBACK.equals(path)) {
      handleCallback(httpRequest, httpResponse, contextPath, config);
      return;
    }
    if (PATH_OAUTH_LOGOUT.equals(path)) {
      handleLogout(httpRequest, httpResponse, contextPath, config);
      return;
    }
    if (HopLoginPage.PATH_LOGIN.equals(path) || (HopLoginPage.PATH_LOGIN + "/").equals(path)) {
      HopAuthenticatedPrincipal existing = sessionPrincipal(httpRequest);
      if (existing != null) {
        httpResponse.sendRedirect(contextPath + "/ui");
        return;
      }
      showLoginPage(httpRequest, httpResponse, contextPath, config, null);
      return;
    }

    HopAuthenticatedPrincipal principal = sessionPrincipal(httpRequest);
    if (principal != null) {
      chain.doFilter(new HopAuthenticatedRequest(httpRequest, principal), response);
      return;
    }

    // Unauthenticated
    if (wantsHtml(httpRequest)) {
      String redirect =
          path + (httpRequest.getQueryString() != null ? "?" + httpRequest.getQueryString() : "");
      if (redirect.isBlank() || "/".equals(redirect)) {
        redirect = "/ui";
      }
      httpResponse.sendRedirect(
          contextPath + HopLoginPage.PATH_LOGIN + "?redirect=" + urlEncode(redirect));
      return;
    }
    httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    httpResponse.setContentType("text/plain; charset=UTF-8");
    httpResponse.getWriter().write("Authentication required (OIDC)");
  }

  private void handleStart(
      HttpServletRequest request,
      HttpServletResponse response,
      String contextPath,
      HopSecurityConfig config)
      throws IOException {
    if (!config.isOauthConfigured()) {
      showLoginPage(
          request,
          response,
          contextPath,
          config,
          "OAuth2 is not configured. Set issuer URL and client ID in Configuration → Security.");
      return;
    }
    try {
      HopOidcClient client = new HopOidcClient(config);
      String redirectUri = resolveRedirectUri(request, contextPath, config);
      String state = HopOidcClient.newState();
      String nonce = HopOidcClient.newNonce();
      String verifier = config.isOauthUsePkce() ? HopOidcClient.newCodeVerifier() : null;
      String challenge = verifier != null ? HopOidcClient.codeChallengeS256(verifier) : null;

      AuthSession auth = new AuthSession();
      auth.state = state;
      auth.nonce = nonce;
      auth.codeVerifier = verifier;
      auth.redirectAfterLogin =
          HopLoginPage.sanitizeRedirect(request.getParameter("redirect"), contextPath);

      HttpSession session = request.getSession(true);
      session.setAttribute(SESSION_AUTH, auth);
      session.removeAttribute(SESSION_PRINCIPAL);

      String authUrl = client.buildAuthorizationUrl(redirectUri, state, nonce, challenge);
      response.sendRedirect(authUrl);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "OIDC start failed", e);
      showLoginPage(
          request, response, contextPath, config, "Could not start SSO: " + e.getMessage());
    }
  }

  private void handleCallback(
      HttpServletRequest request,
      HttpServletResponse response,
      String contextPath,
      HopSecurityConfig config)
      throws IOException {
    String error = request.getParameter("error");
    if (error != null) {
      String desc = request.getParameter("error_description");
      showLoginPage(
          request,
          response,
          contextPath,
          config,
          "SSO error: " + error + (desc != null ? " — " + desc : ""));
      return;
    }

    String code = request.getParameter("code");
    String state = request.getParameter("state");
    HttpSession session = request.getSession(false);
    AuthSession auth = session != null ? (AuthSession) session.getAttribute(SESSION_AUTH) : null;

    if (auth == null || state == null || !state.equals(auth.state)) {
      showLoginPage(request, response, contextPath, config, "Invalid or expired SSO state.");
      return;
    }
    if (code == null || code.isBlank()) {
      showLoginPage(request, response, contextPath, config, "Missing authorization code.");
      return;
    }

    try {
      HopOidcClient client = new HopOidcClient(config);
      String redirectUri = resolveRedirectUri(request, contextPath, config);
      OidcTokenResponse tokens = client.exchangeCode(code, redirectUri, auth.codeVerifier);
      JWTClaimsSet claims = client.validateIdToken(tokens.getIdToken(), auth.nonce);

      HopSecurityContext securityContext = client.toSecurityContext(claims);
      Set<String> roles = client.expandRolesForPrincipal(claims);
      // also include hop role ids from context
      roles.addAll(securityContext.getRoleIds());
      for (String id : securityContext.getRoleIds()) {
        roles.addAll(HopUserStore.expandContainerRoleNames(java.util.List.of(id)));
      }

      HopAuthenticatedPrincipal principal =
          new HopAuthenticatedPrincipal(securityContext.getUsername(), roles);

      session.removeAttribute(SESSION_AUTH);
      session.setAttribute(SESSION_PRINCIPAL, principal);
      session.setAttribute(SESSION_ID_TOKEN, tokens.getIdToken());

      String target =
          auth.redirectAfterLogin != null ? auth.redirectAfterLogin : contextPath + "/ui";
      LOG.info("OIDC sign-in success for user '" + principal.getName() + "'");
      response.sendRedirect(target);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "OIDC callback failed", e);
      showLoginPage(
          request, response, contextPath, config, "SSO sign-in failed: " + e.getMessage());
    }
  }

  private void handleLogout(
      HttpServletRequest request,
      HttpServletResponse response,
      String contextPath,
      HopSecurityConfig config)
      throws IOException {
    HttpSession session = request.getSession(false);
    String idToken = null;
    if (session != null) {
      Object t = session.getAttribute(SESSION_ID_TOKEN);
      if (t instanceof String s) {
        idToken = s;
      }
      session.removeAttribute(SESSION_PRINCIPAL);
      session.removeAttribute(SESSION_AUTH);
      session.removeAttribute(SESSION_ID_TOKEN);
      try {
        session.invalidate();
      } catch (IllegalStateException ignored) {
        // already invalid
      }
    }

    String postLogout = absoluteUrl(request, contextPath + HopLoginPage.PATH_LOGIN);
    try {
      if (config.isOauthConfigured()) {
        HopOidcClient client = new HopOidcClient(config);
        String endSession = client.buildEndSessionUrl(idToken, postLogout);
        if (endSession != null) {
          response.sendRedirect(endSession);
          return;
        }
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "OIDC end-session redirect failed", e);
    }
    response.sendRedirect(contextPath + HopLoginPage.PATH_LOGIN + "?logout=1");
  }

  private void showLoginPage(
      HttpServletRequest request,
      HttpServletResponse response,
      String contextPath,
      HopSecurityConfig config,
      String error)
      throws IOException {
    String redirect = request.getParameter("redirect");
    String html =
        HopLoginPage.renderOauth(
            contextPath,
            config.resolveWelcomeMessage(),
            error,
            redirect,
            config.isOauthConfigured());
    response.setStatus(HttpServletResponse.SC_OK);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType("text/html; charset=UTF-8");
    response.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
    response.getWriter().write(html);
  }

  private void serveCss(HttpServletResponse response) throws IOException {
    String css = HopLoginPage.loadCss();
    response.setStatus(HttpServletResponse.SC_OK);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType("text/css; charset=UTF-8");
    response.setHeader("Cache-Control", "public, max-age=3600");
    response.getWriter().write(css);
  }

  private void serveLogo(HttpServletResponse response) throws IOException {
    byte[] svg = HopLoginPage.loadLogoSvg();
    response.setStatus(HttpServletResponse.SC_OK);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType("image/svg+xml; charset=UTF-8");
    response.setHeader("Cache-Control", "public, max-age=86400");
    response.getOutputStream().write(svg);
  }

  private static HopAuthenticatedPrincipal sessionPrincipal(HttpServletRequest request) {
    HttpSession session = request.getSession(false);
    if (session == null) {
      return null;
    }
    Object attr = session.getAttribute(SESSION_PRINCIPAL);
    return attr instanceof HopAuthenticatedPrincipal p ? p : null;
  }

  static String resolveRedirectUri(
      HttpServletRequest request, String contextPath, HopSecurityConfig config) {
    if (config.getOauthRedirectUri() != null && !config.getOauthRedirectUri().isBlank()) {
      return config.getOauthRedirectUri().trim();
    }
    return absoluteUrl(request, contextPath + PATH_OAUTH_CALLBACK);
  }

  private static String absoluteUrl(HttpServletRequest request, String path) {
    StringBuffer url = request.getRequestURL();
    // requestURL is full URL of current request; rebuild origin
    String scheme = request.getScheme();
    String host = request.getServerName();
    int port = request.getServerPort();
    StringBuilder origin = new StringBuilder();
    origin.append(scheme).append("://").append(host);
    boolean defaultPort =
        ("http".equalsIgnoreCase(scheme) && port == 80)
            || ("https".equalsIgnoreCase(scheme) && port == 443);
    if (!defaultPort && port > 0) {
      origin.append(':').append(port);
    }
    if (!path.startsWith("/")) {
      origin.append('/');
    }
    origin.append(path);
    return origin.toString();
  }

  private static String pathWithinApp(HttpServletRequest request, String contextPath) {
    String uri = request.getRequestURI();
    if (uri == null) {
      return "/";
    }
    if (!contextPath.isEmpty() && uri.startsWith(contextPath)) {
      uri = uri.substring(contextPath.length());
    }
    if (uri.isEmpty()) {
      return "/";
    }
    int semi = uri.indexOf(';');
    if (semi >= 0) {
      uri = uri.substring(0, semi);
    }
    return uri;
  }

  private static boolean wantsHtml(HttpServletRequest request) {
    String accept = request.getHeader("Accept");
    if (accept == null) {
      return true;
    }
    return accept.contains("text/html") || accept.contains("*/*");
  }

  private static String urlEncode(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Override
  public void destroy() {
    // nothing
  }
}
