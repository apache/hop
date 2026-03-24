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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HopServerServletTest {

  private HopServerServlet servlet;
  private Map<String, IHopServerPlugin> registry;

  @BeforeEach
  void setUp() throws Exception {
    HopLogStore.init();
    servlet = new HopServerServlet();
    registry = new ConcurrentHashMap<>();
    Field f = HopServerServlet.class.getDeclaredField("hopServerPluginRegistry");
    f.setAccessible(true);
    f.set(servlet, registry);
  }

  @Test
  void doGetDispatchesToRegisteredPlugin() throws Exception {
    IHopServerPlugin plugin = mock(IHopServerPlugin.class);
    registry.put("/myPlugin", plugin);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getPathInfo()).thenReturn("/myPlugin");
    HttpServletResponse resp = mock(HttpServletResponse.class);

    servlet.doGet(req, resp);

    verify(plugin).doGet(req, resp);
  }

  @Test
  void doGetSendsNotFoundWhenPluginMissing() throws Exception {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getPathInfo()).thenReturn("/unknown");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(resp.isCommitted()).thenReturn(false);

    servlet.doGet(req, resp);

    verify(resp).sendError(eq(HttpServletResponse.SC_NOT_FOUND), eq("Not found."));
  }

  @Test
  void doGetTrimsTrailingSlashOnPath() throws Exception {
    IHopServerPlugin plugin = mock(IHopServerPlugin.class);
    registry.put("/trim", plugin);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getPathInfo()).thenReturn("/trim/");
    HttpServletResponse resp = mock(HttpServletResponse.class);

    servlet.doGet(req, resp);

    verify(plugin).doGet(req, resp);
  }

  @Test
  void doGetHandlesPluginServletException() throws Exception {
    IHopServerPlugin plugin = mock(IHopServerPlugin.class);
    doThrow(new ServletException("plugin failed")).when(plugin).doGet(any(), any());
    registry.put("/bad", plugin);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getPathInfo()).thenReturn("/bad");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(resp.isCommitted()).thenReturn(false);

    servlet.doGet(req, resp);

    verify(resp)
        .sendError(eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR), eq("Plugin request failed."));
  }

  @Test
  void doPostCatchesFailureWhenRegistryUninitialized() throws Exception {
    HopServerServlet bare = new HopServerServlet();
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getPathInfo()).thenReturn("/any");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(resp.isCommitted()).thenReturn(false);

    bare.doPost(req, resp);

    verify(resp)
        .sendError(
            eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
            eq("Unable to process server request."));
  }
}
