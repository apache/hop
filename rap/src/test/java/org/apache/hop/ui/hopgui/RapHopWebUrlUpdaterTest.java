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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;

class RapHopWebUrlUpdaterTest {

  @Test
  void buildQueryEncodesProjectAndFile() {
    String q =
        RapHopWebUrlUpdater.buildQuery(
            "samples", "/usr/local/tomcat/webapps/ROOT/config/projects/samples/loops/wf.hwf");
    assertTrue(q.startsWith("project=samples&file="));
    assertTrue(q.contains("file=%2Fusr%2Flocal%2Ftomcat"));
  }

  @Test
  void requestPathStripsJsessionId() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn("/ui-dark;jsessionid=ABC123");
    assertEquals("/ui-dark", RapHopWebUrlUpdater.requestPath(request));
  }

  @Test
  void publicOriginUsesForwardedProtoAndHost() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getScheme()).thenReturn("http");
    when(request.getServerName()).thenReturn("localhost");
    when(request.getServerPort()).thenReturn(8080);
    when(request.getHeader("X-Forwarded-Proto")).thenReturn("https");
    when(request.getHeader("X-Forwarded-Host")).thenReturn("hop-web.data-hopper.com");
    when(request.getHeader("X-Forwarded-Port")).thenReturn(null);

    assertEquals("https://hop-web.data-hopper.com", RapHopWebUrlUpdater.publicOrigin(request));
  }

  @Test
  void publicOriginUsesForwardedHostWithPort() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getScheme()).thenReturn("http");
    when(request.getServerName()).thenReturn("localhost");
    when(request.getServerPort()).thenReturn(8080);
    when(request.getHeader("X-Forwarded-Proto")).thenReturn("https");
    when(request.getHeader("X-Forwarded-Host")).thenReturn("hop-web.example.com:8443");

    assertEquals("https://hop-web.example.com:8443", RapHopWebUrlUpdater.publicOrigin(request));
  }

  @Test
  void publicOriginFallsBackToRequestWhenNoForwardedHeaders() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getScheme()).thenReturn("http");
    when(request.getServerName()).thenReturn("localhost");
    when(request.getServerPort()).thenReturn(8080);
    when(request.getHeader("X-Forwarded-Proto")).thenReturn(null);
    when(request.getHeader("X-Forwarded-Host")).thenReturn(null);
    when(request.getHeader("X-Forwarded-Port")).thenReturn(null);

    assertEquals("http://localhost:8080", RapHopWebUrlUpdater.publicOrigin(request));
  }

  @Test
  void firstForwardedValueTakesLeftmost() {
    assertEquals("https", RapHopWebUrlUpdater.firstForwardedValue("https, http"));
    assertEquals("a.example.com", RapHopWebUrlUpdater.firstForwardedValue("a.example.com, b"));
  }
}
