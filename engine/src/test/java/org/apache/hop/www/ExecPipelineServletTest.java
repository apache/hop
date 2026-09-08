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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ExecPipelineServletTest {

  private HopServerConfig config;
  private IVariables serverVariables;
  private ExecPipelineServlet servlet;

  @BeforeEach
  void setUp() {
    serverVariables = new Variables();
    serverVariables.setVariable("PROJECT_HOME", "/tmp/issue-8284-home");

    config = new HopServerConfig();
    config.setVariables(serverVariables);
    config.setMetadataProvider(
        new MultiMetadataProvider(Encr.getEncoder(), List.of(), serverVariables));

    PipelineMap pipelineMap = new PipelineMap();
    pipelineMap.setHopServerConfig(config);
    servlet = new ExecPipelineServlet(pipelineMap);
    servlet.setJettyMode(false);
  }

  @Test
  void projectHomeInPipelinePathIsResolvedFromServerVariables() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(out));
    when(request.getParameter("pipeline")).thenReturn("${PROJECT_HOME}/missing.hpl");
    when(request.getParameterNames()).thenReturn(new Vector<>(List.of("pipeline")).elements());

    servlet.doGet(request, response);

    String body = out.toString();
    assertTrue(
        body.contains("/tmp/issue-8284-home/missing.hpl"),
        "resolved path should appear in the error: " + body);
    assertFalse(body.contains("${PROJECT_HOME}/missing.hpl"));
  }

  @Test
  void requestParametersDoNotLeakIntoServerVariableSpace() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter out = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(out));
    when(request.getParameter("pipeline")).thenReturn("${PROJECT_HOME}/missing.hpl");
    when(request.getParameter("FOO")).thenReturn("bar");
    when(request.getParameterValues("FOO")).thenReturn(new String[] {"bar"});
    when(request.getParameterNames())
        .thenReturn(Collections.enumeration(List.of("pipeline", "FOO")));

    servlet.doGet(request, response);

    assertNull(serverVariables.getVariable("FOO"));
    assertEquals("/tmp/issue-8284-home", serverVariables.getVariable("PROJECT_HOME"));
  }
}
