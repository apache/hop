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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the graceful-shutdown behaviour: refusing new work and reporting the shutting-down state.
 */
class GracefulShutdownTest {

  @BeforeEach
  void setup() {
    HopLogStore.init();
    HopServerSingleton.setServerShuttingDown(false);
  }

  @AfterEach
  void tearDown() {
    HopServerSingleton.setServerShuttingDown(false);
  }

  @Test
  void workServletIsRefusedWithServiceUnavailableWhileShuttingDown() throws Exception {
    HopServerSingleton.setServerShuttingDown(true);

    StartPipelineServlet servlet = new StartPipelineServlet(mock(PipelineMap.class));
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getContextPath()).thenReturn(StartPipelineServlet.CONTEXT_PATH);

    servlet.doGet(request, response);

    verify(response).sendError(eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE), anyString());
  }

  @Test
  void workServletIsNotRefusedWhenNotShuttingDown() throws Exception {
    // Not shutting down: the refusal path must not fire (the servlet is free to do its own work).
    assertFalse(HopServerSingleton.isServerShuttingDown());

    AddPipelineServlet servlet = new AddPipelineServlet(mock(PipelineMap.class));
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    // Not the right URI in jetty mode, so the servlet returns early without doing any real work.
    when(request.getRequestURI()).thenReturn("/some/other/path");

    servlet.doGet(request, response);

    verify(response, never())
        .sendError(eq(HttpServletResponse.SC_SERVICE_UNAVAILABLE), anyString());
  }

  @Test
  void statusServletReportsShuttingDownState() throws Exception {
    HopServerSingleton.setServerShuttingDown(true);

    PipelineMap pipelineMap = mock(PipelineMap.class);
    when(pipelineMap.getPipelineObjects()).thenReturn(Collections.emptyList());
    WorkflowMap workflowMap = mock(WorkflowMap.class);
    when(workflowMap.getWorkflowObjects()).thenReturn(Collections.emptyList());

    GetStatusServlet servlet = new GetStatusServlet(pipelineMap, workflowMap);
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(out));
    when(request.getContextPath()).thenReturn(GetStatusServlet.CONTEXT_PATH);
    when(request.getParameter("xml")).thenReturn("Y");

    servlet.doGet(request, response);

    String xml = out.toString();
    assertTrue(xml.contains("Shutting down"), xml);
    assertTrue(xml.contains("<shutting_down>Y</shutting_down>"), xml);
  }
}
