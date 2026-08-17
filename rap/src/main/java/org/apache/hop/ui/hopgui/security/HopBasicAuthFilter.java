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
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hop.core.security.HopSecurityBootstrap;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopUser;
import org.apache.hop.core.security.HopUserStore;

/**
 * Hop-managed authentication filter for mode {@link HopSecurityConfig.AuthMode#BASIC}.
 *
 * <p>Uses a styled form login page ({@link HopLoginPage}) and session-bound principals. Optional
 * HTTP {@code Authorization: Basic} is still accepted for API clients. When mode is not BASIC, the
 * filter is a no-op (EXTERNAL / NONE / container auth).
 */
public class HopBasicAuthFilter implements Filter {

  private static final Logger LOG = Logger.getLogger(HopBasicAuthFilter.class.getName());

  public static final String SESSION_PRINCIPAL = "hop.basic.auth.principal";

  /** Legacy name kept for logout facade compatibility. */
  public static final String SESSION_REJECT_AUTH = "hop.basic.auth.rejectAuth";

  public static final String SESSION_FORCE_REAUTH = "hop.basic.auth.forceReauth";

  public static final String REALM = "Hop Web";

  @Override
  public void init(FilterConfig filterConfig) {
    HopSecurityBootstrap.runOnce();
    HopSecurityConfig.AuthMode mode = HopSecurityConfig.load().getAuthMode();
    LOG.info("HopBasicAuthFilter initialized (mode=" + mode + ")");
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    if (!(request instanceof HttpServletRequest httpRequest)
        || !(response instanceof HttpServletResponse httpResponse)) {
      chain.doFilter(request, response);
      return;
    }

    HopSecurityConfig.AuthMode mode = HopSecurityConfig.load().getAuthMode();
    if (mode != HopSecurityConfig.AuthMode.BASIC) {
      chain.doFilter(request, response);
      return;
    }

    String contextPath = HopLoginPage.normalizeContext(httpRequest.getContextPath());
    String path = pathWithinApp(httpRequest, contextPath);
    HopLoginPage.HttpMethod method = HopLoginPage.HttpMethod.of(httpRequest.getMethod());

    // Login assets and page are public
    if (HopLoginPage.isLoginAssetPath(path)) {
      if (HopLoginPage.PATH_LOGIN_CSS.equals(path)) {
        serveCss(httpResponse);
        return;
      }
      if (HopLoginPage.PATH_LOGIN_LOGO.equals(path)) {
        serveLogo(httpResponse);
        return;
      }
      if (method == HopLoginPage.HttpMethod.POST) {
        handleLoginPost(httpRequest, httpResponse, contextPath);
        return;
      }
      // GET /login — show form (or redirect if already signed in)
      HopAuthenticatedPrincipal existing = resolveSessionPrincipal(httpRequest);
      if (existing != null && !forceReauth(httpRequest)) {
        httpResponse.sendRedirect(contextPath + "/ui");
        return;
      }
      showLoginPage(httpRequest, httpResponse, contextPath, null, null);
      return;
    }

    HopAuthenticatedPrincipal principal = resolvePrincipal(httpRequest);
    if (principal == null) {
      // Prefer form login for browser navigations; API may still use Basic later via 401
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
      challengeBasic(httpResponse);
      return;
    }

    HttpSession session = httpRequest.getSession(true);
    session.setAttribute(SESSION_PRINCIPAL, principal);
    session.removeAttribute(SESSION_FORCE_REAUTH);
    session.removeAttribute(SESSION_REJECT_AUTH);

    chain.doFilter(new HopAuthenticatedRequest(httpRequest, principal), response);
  }

  private void handleLoginPost(
      HttpServletRequest request, HttpServletResponse response, String contextPath)
      throws IOException {
    String username = trim(request.getParameter("username"));
    String password = request.getParameter("password");
    String redirect = request.getParameter("redirect");

    if (username == null || password == null) {
      showLoginPage(
          request, response, contextPath, "Please enter username and password.", username);
      return;
    }

    Optional<HopUser> user = HopUserStore.getInstance().authenticate(username, password);
    if (user.isEmpty()) {
      LOG.log(Level.INFO, "Login failed for user ''{0}''", username);
      showLoginPage(request, response, contextPath, "Invalid username or password.", username);
      return;
    }

    HopUser hopUser = user.get();
    Set<String> roles = HopUserStore.expandContainerRoleNames(hopUser.getRoles());
    HopAuthenticatedPrincipal principal =
        new HopAuthenticatedPrincipal(hopUser.getUsername(), roles);

    HttpSession session = request.getSession(true);
    session.setAttribute(SESSION_PRINCIPAL, principal);
    session.removeAttribute(SESSION_FORCE_REAUTH);
    session.removeAttribute(SESSION_REJECT_AUTH);

    String target = HopLoginPage.sanitizeRedirect(redirect, contextPath);
    LOG.log(Level.INFO, "User ''{0}'' signed in via form login", hopUser.getUsername());
    response.sendRedirect(target);
  }

  private void showLoginPage(
      HttpServletRequest request,
      HttpServletResponse response,
      String contextPath,
      String error,
      String usernamePrefill)
      throws IOException {
    HopSecurityConfig config = HopSecurityConfig.load();
    String redirect = request.getParameter("redirect");
    String html =
        HopLoginPage.render(
            contextPath, config.resolveWelcomeMessage(), error, redirect, usernamePrefill);
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

  private HopAuthenticatedPrincipal resolvePrincipal(HttpServletRequest request) {
    if (forceReauth(request)) {
      HttpSession session = request.getSession(false);
      if (session != null) {
        session.removeAttribute(SESSION_PRINCIPAL);
      }
      // Only accept a fresh Authorization header (API) after force reauth; form login clears flag
      String header = request.getHeader("Authorization");
      if (header != null && header.regionMatches(true, 0, "Basic ", 0, 6)) {
        HopAuthenticatedPrincipal p = authenticateHeader(header);
        if (p != null) {
          return p;
        }
      }
      return null;
    }

    // Session principal from form login
    HopAuthenticatedPrincipal sessionPrincipal = resolveSessionPrincipal(request);
    if (sessionPrincipal != null) {
      return sessionPrincipal;
    }

    // Optional HTTP Basic for API / automation clients
    String header = request.getHeader("Authorization");
    if (header != null && header.regionMatches(true, 0, "Basic ", 0, 6)) {
      return authenticateHeader(header);
    }

    return null;
  }

  private HopAuthenticatedPrincipal resolveSessionPrincipal(HttpServletRequest request) {
    HttpSession session = request.getSession(false);
    if (session == null) {
      return null;
    }
    Object attr = session.getAttribute(SESSION_PRINCIPAL);
    if (attr instanceof HopAuthenticatedPrincipal principal) {
      return principal;
    }
    return null;
  }

  private boolean forceReauth(HttpServletRequest request) {
    HttpSession session = request.getSession(false);
    return session != null && Boolean.TRUE.equals(session.getAttribute(SESSION_FORCE_REAUTH));
  }

  private HopAuthenticatedPrincipal authenticateHeader(String header) {
    try {
      String b64 = header.substring(6).trim();
      String decoded = new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
      int colon = decoded.indexOf(':');
      if (colon < 0) {
        return null;
      }
      String username = decoded.substring(0, colon);
      String password = decoded.substring(colon + 1);
      Optional<HopUser> user = HopUserStore.getInstance().authenticate(username, password);
      if (user.isEmpty()) {
        LOG.log(Level.FINE, "BASIC header auth failed for user ''{0}''", username);
        return null;
      }
      HopUser hopUser = user.get();
      Set<String> roles = HopUserStore.expandContainerRoleNames(hopUser.getRoles());
      return new HopAuthenticatedPrincipal(hopUser.getUsername(), roles);
    } catch (IllegalArgumentException e) {
      LOG.log(Level.FINE, "Invalid BASIC credentials encoding", e);
      return null;
    }
  }

  private void challengeBasic(HttpServletResponse response) throws IOException {
    response.setHeader("WWW-Authenticate", "Basic realm=\"" + REALM + "\"");
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType("text/plain; charset=UTF-8");
    response.getWriter().write("Authentication required");
  }

  private static boolean wantsHtml(HttpServletRequest request) {
    String accept = request.getHeader("Accept");
    if (accept == null) {
      return true;
    }
    return accept.contains("text/html") || accept.contains("*/*");
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
    // strip ;jsessionid=
    int semi = uri.indexOf(';');
    if (semi >= 0) {
      uri = uri.substring(0, semi);
    }
    return uri;
  }

  private static String trim(String s) {
    return s == null ? null : s.trim();
  }

  private static String urlEncode(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Override
  public void destroy() {
    // nothing
  }
}
