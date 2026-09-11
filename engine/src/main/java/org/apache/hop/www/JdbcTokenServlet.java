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
package org.apache.hop.www;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Serial;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.annotations.HopServerServlet;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.security.HopJdbcTokenService;
import org.apache.hop.core.security.HopRole;

/**
 * Issues a short-lived HMAC JWT for JDBC / API clients. The caller must already be authenticated
 * (session cookie or another accepted credential); this does not replace IdP login.
 */
@HopServerServlet(
    id = "jdbcToken",
    name = "Issue a short-lived JDBC Bearer token",
    requiredPermission = "file.view")
public class JdbcTokenServlet extends BaseHttpServlet implements IHopServerPlugin {

  @Serial private static final long serialVersionUID = 1L;

  public static final String CONTEXT_PATH = "/hop/jdbcToken";

  public JdbcTokenServlet() {}

  public JdbcTokenServlet(PipelineMap pipelineMap) {
    super(pipelineMap);
  }

  @Override
  public String getContextPath() {
    return CONTEXT_PATH;
  }

  @Override
  public String getService() {
    return CONTEXT_PATH + " (" + this + ")";
  }

  @Override
  public String toString() {
    return "JDBC token";
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (isJettyMode() && !request.getContextPath().startsWith(CONTEXT_PATH)) {
      return;
    }

    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType("application/json");
    response.setHeader("Cache-Control", "no-store");

    Principal principal = request.getUserPrincipal();
    if (principal == null || principal.getName() == null || principal.getName().isBlank()) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.setHeader("WWW-Authenticate", "Bearer");
      response.getWriter().write("{\"error\":\"Authentication required\"}");
      return;
    }

    Set<String> roles = new LinkedHashSet<>();
    for (HopRole role : HopRole.values()) {
      if (request.isUserInRole(role.getId()) || request.isUserInRole("hop-" + role.getId())) {
        roles.add(role.getId());
      }
    }
    if (roles.isEmpty()) {
      roles.add(HopRole.USER.getId());
    }

    try {
      HopJdbcTokenService.IssuedToken issued =
          HopJdbcTokenService.issue(principal.getName(), roles, HopJdbcTokenService.DEFAULT_TTL);

      Map<String, Object> body = new LinkedHashMap<>();
      body.put("tokenType", "Bearer");
      body.put("token", issued.token());
      body.put("expiresIn", issued.expiresInSeconds());
      body.put("username", principal.getName());

      ObjectMapper mapper = HopJson.newMapper();
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().write(mapper.writeValueAsString(body));
    } catch (Exception e) {
      logError("Failed to issue JDBC token", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().write("{\"error\":\"Failed to issue JDBC token\"}");
    }
  }
}
