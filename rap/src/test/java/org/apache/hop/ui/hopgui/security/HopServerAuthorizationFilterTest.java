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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Set;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.security.HopSecurityConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link HopServerAuthorizationFilter}: verifies that {@code /hop/*} endpoints are
 * gated by the caller's Hop role in the authenticated modes, and left open in mode {@code NONE}.
 */
class HopServerAuthorizationFilterTest {

  @TempDir static Path configFolder;

  private HopServerAuthorizationFilter filter;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private FilterChain chain;
  private StringWriter responseBody;

  @BeforeAll
  static void initEnvironment() {
    // HopSecurityConfig.save() logs and writes; give it a log store and a scratch config folder.
    System.setProperty("HOP_CONFIG_FOLDER", configFolder.toAbsolutePath().toString());
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    filter = new HopServerAuthorizationFilter();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    chain = mock(FilterChain.class);
    responseBody = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(responseBody));
    when(request.getContextPath()).thenReturn("");
  }

  @AfterEach
  void tearDown() {
    setMode(HopSecurityConfig.AuthMode.NONE);
    HopSecurityConfig.clearCache();
  }

  private void setMode(HopSecurityConfig.AuthMode mode) {
    setMode(mode, false);
  }

  private void setMode(HopSecurityConfig.AuthMode mode, boolean allowUnauthenticatedServerApi) {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setAuthMode(mode);
    config.setAllowUnauthenticatedServerApi(allowUnauthenticatedServerApi);
    HopSecurityConfig.save(config);
  }

  /** Wire the request as an authenticated user with the given built-in role names. */
  private void authenticateAs(String username, Set<String> roleNames) {
    Principal principal = () -> username;
    when(request.getUserPrincipal()).thenReturn(principal);
    when(request.isUserInRole(anyString()))
        .thenAnswer(inv -> roleNames.contains(inv.getArgument(0, String.class)));
  }

  private void requestPath(String uri) {
    when(request.getRequestURI()).thenReturn(uri);
  }

  @Test
  void modeNoneClosedByDefault() throws Exception {
    // Default open Hop Web install: the server API is closed so there is no unauthenticated
    // pipeline/workflow execution.
    setMode(HopSecurityConfig.AuthMode.NONE);
    requestPath("/hop/startPipeline");
    filter.doFilter(request, response, chain);
    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  void modeNoneOpenWhenExplicitlyAllowed() throws Exception {
    setMode(HopSecurityConfig.AuthMode.NONE, true);
    requestPath("/hop/startPipeline");
    // No principal at all — passes through when the operator opted in.
    filter.doFilter(request, response, chain);
    verify(chain, times(1)).doFilter(request, response);
    verify(response, never()).setStatus(anyInt());
  }

  @Test
  void readOnlyAllowedOnStatus() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    authenticateAs("viewer", Set.of("readonly"));
    requestPath("/hop/status");
    filter.doFilter(request, response, chain);
    verify(chain, times(1)).doFilter(request, response);
  }

  @Test
  void readOnlyDeniedOnAddWorkflow() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    authenticateAs("viewer", Set.of("readonly"));
    requestPath("/hop/addWorkflow");
    filter.doFilter(request, response, chain);
    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  void readOnlyDeniedOnStartAndRemove() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    for (String path :
        new String[] {"/hop/startWorkflow", "/hop/execWorkflow", "/hop/removePipeline"}) {
      chain = mock(FilterChain.class);
      response = mock(HttpServletResponse.class);
      when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
      authenticateAs("viewer", Set.of("readonly"));
      requestPath(path);
      filter.doFilter(request, response, chain);
      verify(chain, never()).doFilter(any(), any());
      verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
    }
  }

  @Test
  void operatorAllowedToRunButNotToDeploy() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);

    // Run endpoint: allowed (RUN_EXECUTE).
    authenticateAs("operator", Set.of("operator"));
    requestPath("/hop/startPipeline");
    filter.doFilter(request, response, chain);
    verify(chain, times(1)).doFilter(request, response);

    // Deploy endpoint: denied (FILE_SAVE not granted to Operator).
    chain = mock(FilterChain.class);
    response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    authenticateAs("operator", Set.of("operator"));
    requestPath("/hop/addPipeline");
    filter.doFilter(request, response, chain);
    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  void developerAllowedToDeployAndRun() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    authenticateAs("developer", Set.of("user"));
    requestPath("/hop/addPipeline");
    filter.doFilter(request, response, chain);
    verify(chain, times(1)).doFilter(request, response);
  }

  @Test
  void unknownEndpointDeniedByDefault() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    authenticateAs("admin", Set.of("admin"));
    requestPath("/hop/somethingBrandNew");
    filter.doFilter(request, response, chain);
    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  void missingPrincipalInAuthenticatedModeIsUnauthorized() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    when(request.getUserPrincipal()).thenReturn(null);
    requestPath("/hop/startPipeline");
    filter.doFilter(request, response, chain);
    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
  }

  @Test
  void adminAllowedEverywhere() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    for (String path :
        new String[] {
          "/hop/addWorkflow", "/hop/execWorkflow", "/hop/removeWorkflow", "/hop/status"
        }) {
      chain = mock(FilterChain.class);
      HttpServletResponse resp = mock(HttpServletResponse.class);
      when(resp.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
      authenticateAs("admin", Set.of("admin"));
      requestPath(path);
      filter.doFilter(request, resp, chain);
      verify(chain, times(1)).doFilter(request, resp);
    }
  }

  @Test
  void contextPathIsStrippedBeforeMatching() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    when(request.getContextPath()).thenReturn("/hop-web");
    authenticateAs("viewer", Set.of("readonly"));
    when(request.getRequestURI()).thenReturn("/hop-web/hop/addPipeline");
    filter.doFilter(request, response, chain);
    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  void denyResponseCarriesForbiddenStatus() throws Exception {
    setMode(HopSecurityConfig.AuthMode.BASIC);
    authenticateAs("viewer", Set.of("readonly"));
    requestPath("/hop/startWorkflow");
    filter.doFilter(request, response, chain);
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
    assertEquals(true, responseBody.toString().toLowerCase().contains("run.execute"));
  }
}
