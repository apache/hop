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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.logging.ILogChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BaseHttpServletTest {

  private BaseHttpServlet servlet;

  @BeforeEach
  void setUp() {
    servlet = new BaseHttpServlet();
    servlet.setLog(mock(ILogChannel.class));
  }

  @Test
  void supportGraphicEnvironmentGetterSetter() {
    assertFalse(servlet.isSupportGraphicEnvironment());
    servlet.setSupportGraphicEnvironment(true);
    assertTrue(servlet.isSupportGraphicEnvironment());
  }

  @Test
  void convertContextPathJettyVsNonJetty() {
    servlet.setJettyMode(true);
    assertEquals("/hop/status", servlet.convertContextPath("/hop/status"));
    servlet.setJettyMode(false);
    assertEquals("status", servlet.convertContextPath("/hop/status"));
  }

  @Test
  void isJsonRequestDetectsParameter() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter("json")).thenReturn("Y");
    assertTrue(servlet.isJsonRequest(request));
    when(request.getParameter("json")).thenReturn("n");
    assertFalse(servlet.isJsonRequest(request));
  }

  @Test
  void sendSafeErrorWhenCommittedOnlySetsStatus() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(true);
    servlet.sendSafeError(response, HttpServletResponse.SC_BAD_REQUEST, "bad");
    verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
    verify(response, org.mockito.Mockito.never()).sendError(anyInt(), anyString());
  }

  @Test
  void sendSafeErrorFallsBackToSetStatusWhenSendErrorThrows() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(false);
    doThrow(new IOException("broken")).when(response).sendError(anyInt(), anyString());
    servlet.sendSafeError(response, HttpServletResponse.SC_CONFLICT, "msg");
    verify(response).setStatus(HttpServletResponse.SC_CONFLICT);
  }

  @Test
  void writeXmlOrJsonApiErrorWritesXmlToOutputStream() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(false);
    doNothing().when(response).resetBuffer();
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
    when(response.getOutputStream()).thenReturn(sos);

    servlet.writeXmlOrJsonApiError(
        response, null, true, false, "unit test", new IllegalStateException("cause"));

    verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    String body = baos.toString(StandardCharsets.UTF_8);
    assertTrue(body.contains(WebResult.STRING_ERROR));
    assertTrue(body.contains("Unable to complete request."));
  }

  @Test
  void writeXmlOrJsonApiErrorUsesWriterWhenProvided() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(false);
    doNothing().when(response).resetBuffer();
    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);

    servlet.writeXmlOrJsonApiError(
        response, writer, false, true, "unit test json", new RuntimeException("x"));
    writer.flush();

    verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    assertTrue(sw.toString().contains(WebResult.STRING_ERROR));
  }

  @Test
  void getSafeWriterReturnsNullAndSendsErrorWhenGetWriterThrows() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.isCommitted()).thenReturn(false);
    when(response.getWriter()).thenThrow(new IOException("no writer"));
    doNothing()
        .when(response)
        .sendError(eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR), anyString());

    assertNull(servlet.getSafeWriter(response));
    verify(response).sendError(eq(HttpServletResponse.SC_INTERNAL_SERVER_ERROR), anyString());
  }

  @Test
  void getSafeWriterReturnsWriter() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    PrintWriter pw = new PrintWriter(new StringWriter());
    when(response.getWriter()).thenReturn(pw);
    assertNotNull(servlet.getSafeWriter(response));
  }
}
