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
package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.security.HopJdbcTokenService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcTokenServletTest {

  @BeforeAll
  static void initLog() {
    HopLogStore.init();
  }

  @BeforeEach
  void pinSecret() {
    System.setProperty(HopJdbcTokenService.ENV_SECRET, "abcdefghijklmnopqrstuvwxyz012345");
  }

  @AfterEach
  void clearSecret() {
    System.clearProperty(HopJdbcTokenService.ENV_SECRET);
  }

  @Test
  void unauthenticatedRequestIs401WithBearerChallenge() throws Exception {
    JdbcTokenServlet servlet = new JdbcTokenServlet();
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(body));
    when(request.getUserPrincipal()).thenReturn(null);

    servlet.doGet(request, response);

    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(response).setHeader("WWW-Authenticate", "Bearer");
    assertTrue(body.toString().contains("Authentication required"));
  }

  @Test
  void authenticatedRequestIssuesAHopJdbcToken() throws Exception {
    JdbcTokenServlet servlet = new JdbcTokenServlet();
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(body));
    Principal principal = () -> "alice";
    when(request.getUserPrincipal()).thenReturn(principal);
    when(request.isUserInRole(anyString())).thenReturn(false);
    when(request.isUserInRole("admin")).thenReturn(true);

    servlet.doGet(request, response);

    verify(response).setStatus(HttpServletResponse.SC_OK);
    String json = body.toString();
    assertTrue(json.contains("\"tokenType\":\"Bearer\""));
    assertTrue(json.contains("\"username\":\"alice\""));
    int tokenStart = json.indexOf("\"token\":\"") + "\"token\":\"".length();
    int tokenEnd = json.indexOf('"', tokenStart);
    String token = json.substring(tokenStart, tokenEnd);
    assertEquals("alice", HopJdbcTokenService.verify(token).getSubject());
  }
}
