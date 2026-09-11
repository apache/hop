/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
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
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hop.core.security.HopJdbcTokenService;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopSecurityContext;
import org.apache.hop.core.security.HopUserStore;
import org.apache.hop.core.security.oidc.HopOidcClient;

/**
 * Shared Bearer handling for Hop Web filters: Hop-issued JDBC HMAC JWTs, then (in OAUTH2 mode) IdP
 * JWTs via JWKS.
 */
public final class HopBearerSupport {

  private static final Logger LOG = Logger.getLogger(HopBearerSupport.class.getName());

  public static final String WWW_AUTHENTICATE_BEARER = "Bearer";

  private HopBearerSupport() {}

  /**
   * @param request HTTP request
   * @return the Bearer token or null
   */
  public static String bearerToken(HttpServletRequest request) {
    if (request == null) {
      return null;
    }
    String header = request.getHeader("Authorization");
    if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)) {
      return null;
    }
    String token = header.substring(7).trim();
    return token.isEmpty() ? null : token;
  }

  /**
   * Authenticate a Bearer token. Tries Hop JDBC HMAC first, then IdP JWT when OAUTH2 is configured.
   *
   * @param request request
   * @param config security config
   * @return principal or null
   */
  public static HopAuthenticatedPrincipal authenticate(
      HttpServletRequest request, HopSecurityConfig config) {
    String token = bearerToken(request);
    if (token == null) {
      return null;
    }
    try {
      JWTClaimsSet hopClaims = HopJdbcTokenService.verify(token);
      return principalFromHopToken(hopClaims);
    } catch (Exception e) {
      LOG.log(Level.FINE, "Not a Hop JDBC token, trying IdP JWT if configured", e);
    }
    if (config != null
        && config.getAuthMode() == HopSecurityConfig.AuthMode.OAUTH2
        && config.isOauthConfigured()) {
      try {
        HopOidcClient client = new HopOidcClient(config);
        JWTClaimsSet claims = client.validateIdToken(token, null);
        return principalFromOidc(client, claims);
      } catch (Exception e) {
        LOG.log(Level.INFO, "Bearer IdP JWT validation failed", e);
      }
    }
    return null;
  }

  /**
   * 401 with {@code WWW-Authenticate: Bearer} for API clients.
   *
   * @param response response
   */
  public static void challenge(HttpServletResponse response) throws IOException {
    if (response.isCommitted()) {
      return;
    }
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    response.setHeader("WWW-Authenticate", WWW_AUTHENTICATE_BEARER);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType("text/plain; charset=UTF-8");
    response.setHeader("Cache-Control", "no-store");
    response.getWriter().write("Authentication required (Bearer)");
  }

  static HopAuthenticatedPrincipal principalFromHopToken(JWTClaimsSet claims) {
    String username = claims.getSubject();
    Set<String> roles = new LinkedHashSet<>(HopJdbcTokenService.roleNames(claims));
    roles.addAll(HopUserStore.expandContainerRoleNames(roles.stream().toList()));
    if (roles.isEmpty()) {
      roles.add(HopRole.USER.getId());
      roles.add("hop-user");
    }
    return new HopAuthenticatedPrincipal(username, roles);
  }

  static HopAuthenticatedPrincipal principalFromOidc(HopOidcClient client, JWTClaimsSet claims) {
    HopSecurityContext securityContext = client.toSecurityContext(claims);
    Set<String> roles = new LinkedHashSet<>(client.expandRolesForPrincipal(claims));
    roles.addAll(securityContext.getRoleIds());
    for (String id : securityContext.getRoleIds()) {
      roles.addAll(HopUserStore.expandContainerRoleNames(java.util.List.of(id)));
    }
    return new HopAuthenticatedPrincipal(securityContext.getUsername(), roles);
  }
}
