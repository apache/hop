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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopSecurityContextResolver;
import org.apache.hop.core.security.HopServerEndpointPermissionMapper;
import org.apache.hop.core.security.Permission;

/**
 * Role-based access control for the Hop Server API ({@code /hop/*}) co-deployed inside Hop Web.
 *
 * <p>The authentication filters ({@link HopBasicAuthFilter} / {@link HopOidcAuthFilter}) run first
 * and, in the authenticated modes, wrap the request with a principal and roles. This filter maps
 * the requested endpoint to the {@link Permission} it needs via {@link
 * HopServerEndpointPermissionMapper} and rejects the call with {@code 403} when the caller's roles
 * do not grant it. A {@code READ_ONLY} user can therefore read status and images but cannot deploy,
 * run, stop, or remove.
 *
 * <p>Behaviour by mode:
 *
 * <ul>
 *   <li>{@code NONE} — no user identity, so the server API is all-open or all-closed. Closed by
 *       default so the default open Hop Web install does not expose unauthenticated pipeline and
 *       workflow execution; set {@code allowUnauthenticatedServerApi} (or {@code
 *       HOP_WEB_ALLOW_UNAUTHENTICATED_SERVER_API}) to open it behind your own network controls.
 *   <li>{@code EXTERNAL} / {@code BASIC} / {@code OAUTH2} — enforce the endpoint permission against
 *       the authenticated principal's roles.
 * </ul>
 *
 * <p>Unknown {@code /hop/*} paths (a servlet not in the built-in table, e.g. a third-party plugin)
 * are denied by default in the authenticated modes, so a newly added servlet cannot silently widen
 * the authenticated attack surface. It becomes reachable once its path is added to the mapper.
 *
 * <p>This filter does not authenticate; if no principal is present in an authenticated mode the
 * upstream auth filter has already redirected or challenged, so a missing principal here is treated
 * as unauthorized.
 */
public class HopServerAuthorizationFilter implements Filter {

  private static final Logger LOG = Logger.getLogger(HopServerAuthorizationFilter.class.getName());

  @Override
  public void init(FilterConfig filterConfig) {
    LOG.info(
        "HopServerAuthorizationFilter initialized (guards /hop/* RBAC in authenticated modes)");
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    if (!(request instanceof HttpServletRequest httpRequest)
        || !(response instanceof HttpServletResponse httpResponse)) {
      chain.doFilter(request, response);
      return;
    }

    HopSecurityConfig config = HopSecurityConfig.load();
    HopSecurityConfig.AuthMode mode = config.getAuthMode();

    String path = pathWithinApp(httpRequest);
    if (path == null || !path.startsWith("/hop")) {
      // Not a Hop Server endpoint (filter is mapped to /hop/* but stay defensive).
      chain.doFilter(request, response);
      return;
    }

    // Mode NONE has no user identity, so the server API is either fully open or fully closed.
    // Closed by default so the default open Hop Web install does not expose unauthenticated
    // pipeline/workflow execution; opt in with allowUnauthenticatedServerApi.
    if (mode == HopSecurityConfig.AuthMode.NONE) {
      if (config.isAllowUnauthenticatedServerApi()) {
        chain.doFilter(request, response);
      } else {
        LOG.log(
            Level.FINE,
            "Hop Server API disabled in mode NONE for ''{0}'' (allowUnauthenticatedServerApi=false)",
            path);
        deny(
            httpResponse,
            "Hop Server API is disabled. Enable authentication, or set "
                + "allowUnauthenticatedServerApi=true to expose it without authentication.",
            HttpServletResponse.SC_FORBIDDEN);
      }
      return;
    }

    HopSecurityContext context = resolveContext(httpRequest);

    // Unrestricted context (no real principal): the auth filter should have handled this; refuse.
    if (context == null) {
      deny(httpResponse, "Authentication required", HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }

    Optional<Permission> required = HopServerEndpointPermissionMapper.requiredPermission(path);
    if (required.isEmpty()) {
      // Unknown endpoint: default-deny so new/plugin servlets cannot bypass RBAC.
      LOG.log(
          Level.WARNING,
          "Denying unmapped Hop Server endpoint ''{0}'' for user ''{1}'' (default-deny)",
          new Object[] {path, context.getUsername()});
      deny(httpResponse, "Not authorized for this endpoint", HttpServletResponse.SC_FORBIDDEN);
      return;
    }

    if (!context.allows(required.get())) {
      LOG.log(
          Level.INFO,
          "Hop Server RBAC denied ''{0}'' for user ''{1}'' (requires {2}, roles={3})",
          new Object[] {path, context.getUsername(), required.get().getId(), context.getRoleIds()});
      deny(
          httpResponse,
          "Access denied: " + required.get().getId() + " required",
          HttpServletResponse.SC_FORBIDDEN);
      return;
    }

    chain.doFilter(request, response);
  }

  /**
   * Resolve the security context from the request principal and container roles, the same way the
   * RAP UI session provider does. Returns {@code null} when no real principal is present.
   */
  private HopSecurityContext resolveContext(HttpServletRequest request) {
    Principal principal;
    try {
      principal = request.getUserPrincipal();
    } catch (UnsupportedOperationException e) {
      return null;
    }
    if (principal == null || principal.getName() == null || principal.getName().isBlank()) {
      return null;
    }
    String username = principal.getName().trim();
    Set<String> roles =
        HopSecurityContextResolver.collectKnownContainerRoles(request::isUserInRole);
    HopSecurityContext context = HopSecurityContextResolver.resolve(username, roles);
    // A blank principal would have produced an unrestricted context; guard against that leaking
    // full access to the server API.
    if (context == null || context.isUnrestricted()) {
      return null;
    }
    return context;
  }

  private void deny(HttpServletResponse response, String message, int status) throws IOException {
    if (response.isCommitted()) {
      return;
    }
    response.setStatus(status);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType("text/plain; charset=UTF-8");
    response.setHeader("Cache-Control", "no-store");
    response.getWriter().write(message);
  }

  private static String pathWithinApp(HttpServletRequest request) {
    String contextPath = request.getContextPath();
    String uri = request.getRequestURI();
    if (uri == null) {
      return null;
    }
    if (contextPath != null && !contextPath.isEmpty() && uri.startsWith(contextPath)) {
      uri = uri.substring(contextPath.length());
    }
    if (uri.isEmpty()) {
      return "/";
    }
    return uri;
  }

  @Override
  public void destroy() {
    // nothing
  }
}
