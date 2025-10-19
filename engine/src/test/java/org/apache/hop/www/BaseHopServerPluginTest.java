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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.ILogChannel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class BaseHopServerPluginTest {

  HttpServletRequest req = mock(HttpServletRequest.class);
  HttpServletResponse resp = mock(HttpServletResponse.class);
  ILogChannel log = mock(ILogChannel.class);
  IHopServerRequestHandler.IWriterResponse writerResponse =
      mock(IHopServerRequestHandler.IWriterResponse.class);
  IHopServerRequestHandler.IOutputStreamResponse outputStreamResponse =
      mock(IHopServerRequestHandler.IOutputStreamResponse.class);
  PrintWriter printWriter = mock(PrintWriter.class);
  jakarta.servlet.ServletOutputStream outputStream =
      mock(jakarta.servlet.ServletOutputStream.class);

  ArgumentCaptor<IHopServerRequestHandler.IHopServerRequest> carteReqCaptor =
      ArgumentCaptor.forClass(IHopServerRequestHandler.IHopServerRequest.class);

  BaseHopServerPlugin baseHopServerPlugin;

  @Before
  public void before() {
    baseHopServerPlugin =
        spy(
            new BaseHopServerPlugin() {
              @Override
              public void handleRequest(IHopServerRequest request) throws IOException {
                // Do nothing
              }

              @Override
              public String getContextPath() {
                return null;
              }
            });
    baseHopServerPlugin.log = log;
  }

  @Test
  public void testDoGet() throws Exception {
    baseHopServerPlugin.doGet(req, resp);
    // doGet should delegate to .service
    verify(baseHopServerPlugin).service(req, resp);
  }

  @Test
  public void testService() throws Exception {
    when(req.getContextPath()).thenReturn("/Path");
    when(baseHopServerPlugin.getContextPath()).thenReturn("/Path");
    when(log.isDebug()).thenReturn(true);

    baseHopServerPlugin.service(req, resp);

    verify(log).logDebug(baseHopServerPlugin.getService());
    verify(baseHopServerPlugin).handleRequest(carteReqCaptor.capture());

    IHopServerRequestHandler.IHopServerRequest carteRequest = carteReqCaptor.getValue();

    testHopServerRequest(carteRequest);
    testHopServerResponse(carteRequest.respond(200));
  }

  private void testHopServerResponse(IHopServerRequestHandler.IHopServerResponse response)
      throws IOException {
    when(resp.getWriter()).thenReturn(printWriter);
    when(resp.getOutputStream()).thenReturn(outputStream);

    response.with("text/xml", writerResponse);

    verify(resp).setContentType("text/xml");
    verify(writerResponse).write(printWriter);

    response.with("text/sgml", outputStreamResponse);

    verify(resp).setContentType("text/sgml");
    verify(outputStreamResponse).write(outputStream);

    response.withMessage("Message");
    verify(resp).setContentType("text/plain");
    verify(printWriter).println("Message");
  }

  private void testHopServerRequest(IHopServerRequestHandler.IHopServerRequest carteRequest) {
    when(req.getMethod()).thenReturn("POST");
    when(req.getHeader("Connection")).thenReturn("Keep-Alive");
    when(req.getParameter("param1")).thenReturn("val1");
    when(req.getParameterNames())
        .thenReturn(Collections.enumeration(Arrays.asList("name1", "name2")));
    when(req.getParameterValues(any(String.class))).thenReturn(new String[] {"val"});
    when(req.getHeaderNames()).thenReturn(Collections.enumeration(Arrays.asList("name1", "name2")));
    when(req.getHeaders("name1")).thenReturn(Collections.enumeration(List.of("val")));
    when(req.getHeaders("name2")).thenReturn(Collections.enumeration(List.of("val")));

    assertEquals("POST", carteRequest.getMethod());
    assertEquals("Keep-Alive", carteRequest.getHeader("Connection"));
    assertEquals("val1", carteRequest.getParameter("param1"));

    checkMappedVals(carteRequest.getParameters());
    checkMappedVals(carteRequest.getHeaders());
  }

  private void checkMappedVals(Map<String, Collection<String>> map) {
    assertEquals(2, map.size());
    Collection<String> name1Params = map.get("name1");
    Collection<String> name2Params = map.get("name2");
    assertEquals(true, name1Params.contains("val"));
    assertEquals(true, name2Params.contains("val"));
    assertEquals(name1Params.size(), name2Params.size());
  }

  @Test
  public void testGetService() {
    when(baseHopServerPlugin.getContextPath()).thenReturn("/Path");
    assertEquals(true, baseHopServerPlugin.getService().startsWith("/Path"));
  }
}
