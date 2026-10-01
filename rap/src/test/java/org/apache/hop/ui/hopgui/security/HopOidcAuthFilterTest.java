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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.security.HopJdbcTokenService;
import org.apache.hop.core.security.HopSecurityConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HopOidcAuthFilterTest {

  private HopOidcAuthFilter filter;
  private HttpServletRequest request;
  private HttpServletResponse response;
  private FilterChain chain;

  @BeforeAll
  static void initLog() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    System.setProperty(HopJdbcTokenService.ENV_SECRET, "abcdefghijklmnopqrstuvwxyz012345");

    HopSecurityConfig config = new HopSecurityConfig();
    config.setAuthMode(HopSecurityConfig.AuthMode.OAUTH2);
    HopSecurityConfig.save(config);

    filter = new HopOidcAuthFilter();
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    chain = mock(FilterChain.class);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    when(request.getContextPath()).thenReturn("");
    when(request.getRequestURI()).thenReturn("/hop/status");
    when(request.getMethod()).thenReturn("GET");
    when(request.getHeader("Accept")).thenReturn("application/json");
    when(request.getSession(false)).thenReturn(null);
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(HopJdbcTokenService.ENV_SECRET);
    HopSecurityConfig config = new HopSecurityConfig();
    config.setAuthMode(HopSecurityConfig.AuthMode.NONE);
    HopSecurityConfig.save(config);
    HopSecurityConfig.clearCache();
  }

  @Test
  void hopJdbcBearerIsAcceptedInOauth2Mode() throws Exception {
    HopJdbcTokenService.IssuedToken issued =
        HopJdbcTokenService.issue("alice", List.of("admin"), Duration.ofMinutes(5));
    when(request.getHeader("Authorization")).thenReturn("Bearer " + issued.token());

    filter.doFilter(request, response, chain);

    ArgumentCaptor<ServletRequest> captor = ArgumentCaptor.forClass(ServletRequest.class);
    verify(chain).doFilter(captor.capture(), eq(response));
    HopAuthenticatedRequest wrapped = (HopAuthenticatedRequest) captor.getValue();
    assertEquals("alice", wrapped.getUserPrincipal().getName());
    verify(request, never()).getSession(true);
  }

  @Test
  void garbageBearerReturns401EvenWhenASessionExists() throws Exception {
    when(request.getHeader("Authorization")).thenReturn("Bearer a.b.c");
    HttpSession session = mock(HttpSession.class);
    when(session.getAttribute(HopOidcAuthFilter.SESSION_PRINCIPAL))
        .thenReturn(new HopAuthenticatedPrincipal("alice", Set.of("admin")));
    when(request.getSession(false)).thenReturn(session);

    filter.doFilter(request, response, chain);

    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(response).setHeader("WWW-Authenticate", HopBearerSupport.WWW_AUTHENTICATE_BEARER);
  }
}
