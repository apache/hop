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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.List;
import org.apache.hop.core.security.HopJdbcTokenService;
import org.apache.hop.core.security.HopSecurityConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopBearerSupportTest {

  @BeforeEach
  void pinSecret() {
    System.setProperty(HopJdbcTokenService.ENV_SECRET, "abcdefghijklmnopqrstuvwxyz012345");
  }

  @AfterEach
  void clearSecret() {
    System.clearProperty(HopJdbcTokenService.ENV_SECRET);
  }

  @Test
  void extractsBearerToken() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("Authorization")).thenReturn("Bearer abc.def.ghi");
    assertEquals("abc.def.ghi", HopBearerSupport.bearerToken(request));
  }

  @Test
  void authenticatesAHopJdbcToken() throws Exception {
    HopJdbcTokenService.IssuedToken issued =
        HopJdbcTokenService.issue("alice", List.of("admin"), Duration.ofMinutes(5));
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("Authorization")).thenReturn("Bearer " + issued.token());

    HopSecurityConfig config = new HopSecurityConfig();
    config.setAuthMode(HopSecurityConfig.AuthMode.BASIC);
    HopAuthenticatedPrincipal principal = HopBearerSupport.authenticate(request, config);
    assertEquals("alice", principal.getName());
  }

  @Test
  void rejectsGarbageBearer() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("Authorization")).thenReturn("Bearer a.b.c");

    HopSecurityConfig config = new HopSecurityConfig();
    config.setAuthMode(HopSecurityConfig.AuthMode.BASIC);
    assertNull(HopBearerSupport.authenticate(request, config));
  }

  @Test
  void challengeSetsWwwAuthenticateBearer() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(body));

    HopBearerSupport.challenge(response);

    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(response).setHeader("WWW-Authenticate", HopBearerSupport.WWW_AUTHENTICATE_BEARER);
  }
}
