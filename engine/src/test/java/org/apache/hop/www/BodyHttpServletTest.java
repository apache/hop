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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BodyHttpServletTest {

  @BeforeEach
  void initLog() {
    HopLogStore.init();
  }

  static final class OkBodyServlet extends BodyHttpServlet {
    @Override
    WebResult generateBody(
        HttpServletRequest request,
        HttpServletResponse response,
        boolean useXML,
        IVariables variables) {
      return WebResult.OK;
    }

    @Override
    public String getContextPath() {
      return "/hop/okBody";
    }
  }

  static final class FailingBodyServlet extends BodyHttpServlet {
    @Override
    WebResult generateBody(
        HttpServletRequest request,
        HttpServletResponse response,
        boolean useXML,
        IVariables variables) {
      throw new RuntimeException("boom");
    }

    @Override
    public String getContextPath() {
      return "/hop/failBody";
    }
  }

  @Test
  void doGetReturnsJsonWhenRequested() throws IOException {
    OkBodyServlet servlet = new OkBodyServlet();
    servlet.setJettyMode(true);
    servlet.setLog(org.mockito.Mockito.mock(org.apache.hop.core.logging.ILogChannel.class));

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getContextPath()).thenReturn("/hop/okBody");
    when(request.getParameter("xml")).thenReturn(null);
    when(request.getParameter("json")).thenReturn("Y");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ServletOutputStream sos =
        new ServletOutputStream() {
          @Override
          public boolean isReady() {
            return true;
          }

          @Override
          public void setWriteListener(WriteListener writeListener) {}

          @Override
          public void write(int b) throws IOException {
            baos.write(b);
          }
        };

    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getOutputStream()).thenReturn(sos);

    servlet.doGet(request, response);

    String body = baos.toString(StandardCharsets.UTF_8);
    assertTrue(body.contains(WebResult.STRING_OK));
  }

  @Test
  void doGetReturnsErrorJsonWhenGenerateBodyFails() throws IOException {
    FailingBodyServlet servlet = new FailingBodyServlet();
    servlet.setJettyMode(true);
    servlet.setLog(org.mockito.Mockito.mock(org.apache.hop.core.logging.ILogChannel.class));

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getContextPath()).thenReturn("/hop/failBody");
    when(request.getParameter("json")).thenReturn("Y");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ServletOutputStream sos =
        new ServletOutputStream() {
          @Override
          public boolean isReady() {
            return true;
          }

          @Override
          public void setWriteListener(WriteListener writeListener) {}

          @Override
          public void write(int b) throws IOException {
            baos.write(b);
          }
        };

    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getOutputStream()).thenReturn(sos);

    servlet.doGet(request, response);

    assertTrue(baos.toString(StandardCharsets.UTF_8).contains(WebResult.STRING_ERROR));
  }
}
