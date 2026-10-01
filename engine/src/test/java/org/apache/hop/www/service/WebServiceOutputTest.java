/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.www.api.v1.StreamingWebServiceOutput;
import org.junit.jupiter.api.Test;

/**
 * The two transports differ only in what they can do with the content type and the status code, so
 * that difference is what these pin down.
 */
class WebServiceOutputTest {

  @Test
  void theServletOutputDrivesTheResponseDirectly() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    ServletWebServiceOutput output = new ServletWebServiceOutput(response);

    output.setContentType("application/json", "UTF-8");
    output.setStatus(201);

    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");
    verify(response).setStatus(201);
  }

  @Test
  void theServletOutputReturnsTheResponseStream() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    jakarta.servlet.ServletOutputStream stream = mock(jakarta.servlet.ServletOutputStream.class);
    when(response.getOutputStream()).thenReturn(stream);

    assertSame(stream, new ServletWebServiceOutput(response).getOutputStream());
  }

  @Test
  void theStreamingOutputWritesToTheStreamItWasGiven() throws Exception {
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    StreamingWebServiceOutput output = new StreamingWebServiceOutput(sink, null);

    OutputStream returned = output.getOutputStream();
    returned.write("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    assertSame(sink, returned);
    assertEquals("hello", sink.toString(java.nio.charset.StandardCharsets.UTF_8));
  }

  @Test
  void theStreamingOutputCannotChangeAnAlreadyCommittedResponse() {
    // Content type and status are fixed by JAX-RS before the body streams; these must be no-ops
    // rather than an attempt to set a header that would throw or be silently lost.
    ByteArrayOutputStream sink = new ByteArrayOutputStream();
    StreamingWebServiceOutput output = new StreamingWebServiceOutput(sink, null);

    output.setContentType("application/json", "UTF-8");
    output.setStatus(200);

    assertEquals(0, sink.size());
  }

  @Test
  void aNonDefaultStatusIsLoggedOnceSoTheLimitationIsVisible() {
    ILogChannel log = mock(ILogChannel.class);
    StreamingWebServiceOutput output =
        new StreamingWebServiceOutput(new ByteArrayOutputStream(), log);

    output.setStatus(404);
    output.setStatus(500);
    output.setStatus(503);

    // Once, not once per row: a pipeline emitting thousands of rows must not flood the log.
    verify(log).logDetailed(org.mockito.ArgumentMatchers.contains("404"));
  }

  @Test
  void a200StatusIsNotWorthLogging() {
    ILogChannel log = mock(ILogChannel.class);
    StreamingWebServiceOutput output =
        new StreamingWebServiceOutput(new ByteArrayOutputStream(), log);

    output.setStatus(200);

    verify(log, org.mockito.Mockito.never()).logDetailed(org.mockito.ArgumentMatchers.anyString());
  }
}
