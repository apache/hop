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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.hop.core.logging.HopLogStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GetWorkflowImageServletTest {

  @BeforeEach
  void initLog() {
    HopLogStore.init();
  }

  @Test
  void doGetReturnsEarlyWhenContextPathDoesNotMatchInJettyMode()
      throws IOException, ServletException {
    GetWorkflowImageServlet servlet = new GetWorkflowImageServlet();
    servlet.setJettyMode(true);
    servlet.setLog(mock(org.apache.hop.core.logging.ILogChannel.class));

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getContextPath()).thenReturn("/wrong");
    HttpServletResponse response = mock(HttpServletResponse.class);

    servlet.doGet(request, response);

    verifyNoInteractions(response);
  }

  @Test
  void doGetReturnsNoContentWhenGraphicsDisabled() throws IOException, ServletException {
    GetWorkflowImageServlet servlet = new GetWorkflowImageServlet();
    servlet.setJettyMode(true);
    servlet.setSupportGraphicEnvironment(false);
    servlet.setLog(mock(org.apache.hop.core.logging.ILogChannel.class));

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getContextPath()).thenReturn(GetWorkflowImageServlet.CONTEXT_PATH);
    HttpServletResponse response = mock(HttpServletResponse.class);

    servlet.doGet(request, response);

    verify(response).setStatus(HttpServletResponse.SC_NO_CONTENT);
  }
}
