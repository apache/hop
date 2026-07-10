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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShutdownServletTest {

  @BeforeEach
  void setup() {
    HopLogStore.init();
  }

  @AfterEach
  void tearDown() {
    HopServerSingleton.setHopServer(null);
  }

  @Test
  void doGetReturnsOkAndTriggersShutdown() throws Exception {
    HopServer server = mock(HopServer.class);
    HopServerSingleton.setHopServer(server);

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getRemoteUser()).thenReturn("tester");
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");

    new ShutdownServlet().doGet(request, response);

    // The servlet responds immediately and schedules the shutdown on a short delayed timer.
    verify(response).setStatus(HttpServletResponse.SC_OK);
    verify(server, timeout(15000)).shutdown();
  }

  @Test
  void doGetDoesNotFailWhenNoServerRegistered() throws Exception {
    HopServer server = mock(HopServer.class);
    // Intentionally not registered in the singleton.

    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getRemoteUser()).thenReturn("tester");
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");

    new ShutdownServlet().doGet(request, response);

    verify(response).setStatus(HttpServletResponse.SC_OK);
    verifyNoInteractions(server);
  }
}
