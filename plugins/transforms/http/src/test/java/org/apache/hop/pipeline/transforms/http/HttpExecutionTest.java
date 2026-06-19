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

package org.apache.hop.pipeline.transforms.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration-style tests for {@link Http#callHttpService} using an embedded HTTP server. */
class HttpExecutionTest {

  private static HttpServer server;
  private static int port;

  // Capture values written by server handlers so assertions can read them
  private static final AtomicReference<String> lastRequestUri = new AtomicReference<>();
  private static final AtomicReference<String> lastRequestHeader = new AtomicReference<>();

  @BeforeAll
  static void startServer() throws Exception {
    HopClientEnvironment.init();

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    port = server.getAddress().getPort();

    // Returns a fixed payload
    server.createContext(
        "/body",
        exchange -> {
          byte[] response = "hello world".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
          }
          exchange.close();
        });

    // Records query string so tests can assert on it
    server.createContext(
        "/query",
        exchange -> {
          lastRequestUri.set(exchange.getRequestURI().toString());
          byte[] response = "ok".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
          }
          exchange.close();
        });

    // Echoes back the custom request header
    server.createContext(
        "/headercheck",
        exchange -> {
          lastRequestHeader.set(exchange.getRequestHeaders().getFirst("X-Custom-Header"));
          byte[] response = "header ok".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
          }
          exchange.close();
        });

    // Returns a custom response header so tests can verify header capture
    server.createContext(
        "/withheader",
        exchange -> {
          exchange.getResponseHeaders().add("X-Custom-Response", "my-value");
          byte[] response = "ok".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
          }
          exchange.close();
        });

    // Returns 200 but no entity
    server.createContext(
        "/nobody",
        exchange -> {
          exchange.sendResponseHeaders(200, 0);
          exchange.close();
        });

    // Returns 204 No Content
    server.createContext(
        "/nocontent",
        exchange -> {
          exchange.sendResponseHeaders(204, -1);
          exchange.close();
        });

    // Returns 401 Unauthorized
    server.createContext(
        "/unauthorized",
        exchange -> {
          exchange.sendResponseHeaders(401, -1);
          exchange.close();
        });

    server.start();
  }

  @AfterAll
  static void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @BeforeEach
  void resetCaptures() {
    lastRequestUri.set(null);
    lastRequestHeader.set(null);
  }

  // ---- helper factories ----

  private String baseUrl() {
    return "http://localhost:" + port;
  }

  private Http newHttp(HttpMeta meta, HttpData data) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("HttpExecTest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("HttpExecTest");
    pipelineMeta.addTransform(transformMeta);
    return new Http(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
  }

  /** Minimal HttpMeta + HttpData for a no-params GET returning just the body field. */
  private Http minimalGet(String url) {
    HttpMeta meta = new HttpMeta(); // resultFields.fieldName = "result" by default
    HttpData data = new HttpData();
    data.realUrl = url;
    data.argNrs = new int[0];
    return newHttp(meta, data);
  }

  // ---- tests ----

  @Test
  void callHttpServiceReturnsBodyAndTracksDataVolumeIn() throws Exception {
    Http http = minimalGet(baseUrl() + "/body");

    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals("hello world", result[0]);
    assertEquals(
        (long) "hello world".getBytes(StandardCharsets.UTF_8).length, getDataVolumeIn(http));
  }

  @Test
  void callHttpServiceWith204ReturnsEmptyBody() throws Exception {
    Http http = minimalGet(baseUrl() + "/nocontent");

    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals("", result[0]);
    assertNull(getDataVolumeIn(http)); // nothing was read
  }

  @Test
  void callHttpServiceWithEmptyEntityReturnsEmptyBody() throws Exception {
    Http http = minimalGet(baseUrl() + "/nobody");

    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    assertEquals("", result[0]);
  }

  @Test
  void callHttpServiceWith401ThrowsHopException() {
    Http http = minimalGet(baseUrl() + "/unauthorized");

    assertThrows(HopException.class, () -> http.callHttpService(new RowMeta(), new Object[0]));
  }

  @Test
  void callHttpServiceWithQueryParameterAppendsToUrl() throws Exception {
    HttpMeta meta = new HttpMeta();
    meta.getLookupParameters().getQueryParameters().add(new HttpMeta.QueryParameter("qField", "q"));

    HttpData data = new HttpData();
    data.realUrl = baseUrl() + "/query";
    data.argNrs = new int[] {0};

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("qField"));

    Http http = newHttp(meta, data);
    http.callHttpService(rowMeta, new Object[] {"searchterm"});

    assertNotNull(lastRequestUri.get());
    assertTrue(lastRequestUri.get().contains("q=searchterm"));
  }

  @Test
  void callHttpServiceSendsRequestHeaderFromRow() throws Exception {
    HttpMeta meta = new HttpMeta();
    HttpData data = new HttpData();
    data.realUrl = baseUrl() + "/headercheck";
    data.argNrs = new int[0];
    data.useHeaderParameters = true;
    data.headerParameters =
        new NameValuePair[] {
          new BasicNameValuePair("X-Custom-Header", "" /* value comes from row */)
        };
    data.headerParametersNrs = new int[] {0};

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("headerValueField"));
    data.inputRowMeta = rowMeta;

    Http http = newHttp(meta, data);
    http.callHttpService(rowMeta, new Object[] {"testvalue"});

    assertEquals("testvalue", lastRequestHeader.get());
  }

  @Test
  void callHttpServiceStatusCodeAppearsInResultWhenConfigured() throws Exception {
    HttpMeta meta = new HttpMeta();
    meta.getResultFields().setResultCodeFieldName("statusCode");

    HttpData data = new HttpData();
    data.realUrl = baseUrl() + "/body";
    data.argNrs = new int[0];

    Http http = newHttp(meta, data);
    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    // result[0] = body (fieldName="result" by default), result[1] = status code
    assertEquals("hello world", result[0]);
    assertEquals(200L, result[1]);
  }

  @Test
  void callHttpServiceResponseHeadersIncludedInResult() throws Exception {
    HttpMeta meta = new HttpMeta();
    meta.getResultFields().setFieldName(""); // no body field
    meta.getResultFields().setResponseHeaderFieldName("responseHeaders");

    HttpData data = new HttpData();
    data.realUrl = baseUrl() + "/withheader";
    data.argNrs = new int[0];

    Http http = newHttp(meta, data);
    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    // result[0] = header JSON (body field empty → skipped)
    String headerJson = (String) result[0];
    assertNotNull(headerJson);
    // Header JSON must contain our custom value (header name casing may vary by HTTP stack)
    assertTrue(
        headerJson.contains("my-value"),
        "Expected headers JSON to contain 'my-value' but got: " + headerJson);
  }

  @Test
  void callHttpServiceWithUnknownHostThrowsHopException() {
    Http http = minimalGet("http://this.host.does.not.exist.invalid");

    assertThrows(HopException.class, () -> http.callHttpService(new RowMeta(), new Object[0]));
  }

  @Test
  void callHttpServiceResponseTimeAppearsInResultWhenConfigured() throws Exception {
    HttpMeta meta = new HttpMeta();
    meta.getResultFields().setResponseTimeFieldName("responseTime");

    HttpData data = new HttpData();
    data.realUrl = baseUrl() + "/body";
    data.argNrs = new int[0];

    Http http = newHttp(meta, data);
    Object[] result = http.callHttpService(new RowMeta(), new Object[0]);

    // result[0] = body, result[1] = responseTime (Long >= 0)
    assertTrue(result[1] instanceof Long);
    assertTrue((Long) result[1] >= 0L);
  }

  // ---- reflection helpers ----

  private static Long getDataVolumeIn(Http http) throws Exception {
    Field field = BaseTransform.class.getDeclaredField("dataVolumeIn");
    field.setAccessible(true);
    return (Long) field.get(http);
  }
}
