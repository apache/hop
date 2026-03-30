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

package org.apache.hop.pipeline.transforms.httppost;

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

/** Integration-style tests for {@link HttpPost#callHttpPOST} using an embedded HTTP server. */
class HttpPostExecutionTest {

  private static HttpServer server;
  private static int port;

  private static final AtomicReference<String> lastRequestBody = new AtomicReference<>();
  private static final AtomicReference<String> lastRequestContentType = new AtomicReference<>();

  @BeforeAll
  static void startServer() throws Exception {
    HopClientEnvironment.init();

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    port = server.getAddress().getPort();

    // Captures request body and echoes it back
    server.createContext(
        "/echo",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          lastRequestBody.set(new String(body, StandardCharsets.UTF_8));
          lastRequestContentType.set(exchange.getRequestHeaders().getFirst("Content-type"));
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
          exchange.close();
        });

    // Returns a fixed body payload
    server.createContext(
        "/body",
        exchange -> {
          byte[] response = "post response".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
          }
          exchange.close();
        });

    // Returns two values for the same response header
    server.createContext(
        "/multiheader",
        exchange -> {
          exchange.getResponseHeaders().add("X-Resp", "val1");
          exchange.getResponseHeaders().add("X-Resp", "val2");
          byte[] response = "ok".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
          }
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
    lastRequestBody.set(null);
    lastRequestContentType.set(null);
  }

  // ---- helpers ----

  private String baseUrl() {
    return "http://localhost:" + port;
  }

  private HttpPost newPost(HttpPostMeta meta, HttpPostData data) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("HttpPostExecTest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("HttpPostExecTest");
    pipelineMeta.addTransform(transformMeta);
    return new HttpPost(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
  }

  /**
   * Minimal meta with a named body result field and no status/time/header fields. The default
   * constructor sets code="result", so we clear that to avoid an extra index in the output.
   */
  private HttpPostMeta bodyOnlyMeta() {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    meta.getResultFields().get(0).setName("response");
    meta.getResultFields().get(0).setCode(null);
    return meta;
  }

  /** Minimal data pointing at the given URL with an empty input row. */
  private HttpPostData minimalData(String url) {
    HttpPostData data = new HttpPostData();
    data.realUrl = url;
    data.inputRowMeta = new RowMeta();
    data.outputRowMeta = new RowMeta();
    return data;
  }

  // ---- tests ----

  @Test
  void callHttpPOSTReturnsBodyAndTracksDataVolumeIn() throws Exception {
    HttpPost http = newPost(bodyOnlyMeta(), minimalData(baseUrl() + "/body"));

    Object[] result = http.callHttpPOST(new Object[0]);

    assertEquals("post response", result[0]);
    assertEquals(
        (long) "post response".getBytes(StandardCharsets.UTF_8).length, getDataVolumeIn(http));
  }

  @Test
  void callHttpPOSTWith204ReturnsEmptyBody() throws Exception {
    HttpPost http = newPost(bodyOnlyMeta(), minimalData(baseUrl() + "/nocontent"));

    Object[] result = http.callHttpPOST(new Object[0]);

    assertEquals("", result[0]);
    assertNull(getDataVolumeIn(http)); // nothing was read
  }

  @Test
  void callHttpPOSTWith401ThrowsHopException() {
    HttpPost http = newPost(bodyOnlyMeta(), minimalData(baseUrl() + "/unauthorized"));

    assertThrows(HopException.class, () -> http.callHttpPOST(new Object[0]));
  }

  @Test
  void callHttpPOSTWithBodyParamsSendsFormEncodedPayload() throws Exception {
    HttpPostMeta meta = bodyOnlyMeta();
    HttpPostData data = minimalData(baseUrl() + "/echo");
    data.useBodyParameters = true;
    data.body_parameters_nrs = new int[] {0};
    data.bodyParameters =
        new NameValuePair[] {
          new BasicNameValuePair("city", "") // name fixed; value comes from row
        };
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("cityField"));
    data.inputRowMeta = rowMeta;
    data.outputRowMeta = rowMeta;

    HttpPost http = newPost(meta, data);
    http.callHttpPOST(new Object[] {"Berlin"});

    assertNotNull(lastRequestBody.get());
    assertTrue(lastRequestBody.get().contains("city=Berlin"));
  }

  @Test
  void callHttpPOSTWithQueryParamsSendsFormEncodedPayload() throws Exception {
    // In HttpPost "query params" are sent as URL-encoded form body (UrlEncodedFormEntity)
    HttpPostMeta meta = bodyOnlyMeta();
    HttpPostData data = minimalData(baseUrl() + "/echo");
    data.useQueryParameters = true;
    data.query_parameters_nrs = new int[] {0};
    data.queryParameters = new NameValuePair[] {new BasicNameValuePair("q", "")};
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("qField"));
    data.inputRowMeta = rowMeta;
    data.outputRowMeta = rowMeta;

    HttpPost http = newPost(meta, data);
    http.callHttpPOST(new Object[] {"test"});

    assertNotNull(lastRequestBody.get());
    assertTrue(lastRequestBody.get().contains("q=test"));
  }

  @Test
  void callHttpPOSTWithRawRequestEntitySendsBytes() throws Exception {
    HttpPostMeta meta = bodyOnlyMeta();
    HttpPostData data = minimalData(baseUrl() + "/echo");
    data.indexOfRequestEntity = 0;
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("bodyField"));
    data.inputRowMeta = rowMeta;
    data.outputRowMeta = rowMeta;

    HttpPost http = newPost(meta, data);
    http.callHttpPOST(new Object[] {"raw body content"});

    assertEquals("raw body content", lastRequestBody.get());
    assertEquals(
        (long) "raw body content".getBytes(StandardCharsets.UTF_8).length, getDataVolumeOut(http));
  }

  @Test
  void callHttpPOSTStatusCodeAppearsInResultWhenConfigured() throws Exception {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    meta.getResultFields().get(0).setName("response");
    meta.getResultFields().get(0).setCode("status");

    HttpPost http = newPost(meta, minimalData(baseUrl() + "/body"));
    Object[] result = http.callHttpPOST(new Object[0]);

    assertEquals("post response", result[0]);
    assertEquals(200L, result[1]);
  }

  @Test
  void callHttpPOSTWithDuplicateResponseHeadersAggregatesIntoArray() throws Exception {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    meta.getResultFields().get(0).setName(null);
    meta.getResultFields().get(0).setCode(null);
    meta.getResultFields().get(0).setResponseHeaderFieldName("headers");

    HttpPost http = newPost(meta, minimalData(baseUrl() + "/multiheader"));
    Object[] result = http.callHttpPOST(new Object[0]);

    // result[0] = headers JSON (name/code both empty → skipped)
    String headersJson = (String) result[0];
    assertNotNull(headersJson);
    // Both header values must appear (name casing/folding varies by HTTP stack)
    assertTrue(
        headersJson.contains("val1"),
        "Expected headers JSON to contain 'val1' but got: " + headersJson);
    assertTrue(
        headersJson.contains("val2"),
        "Expected headers JSON to contain 'val2' but got: " + headersJson);
  }

  @Test
  void callHttpPOSTWithUnknownHostThrowsHopException() {
    HttpPost http = newPost(bodyOnlyMeta(), minimalData("http://this.host.does.not.exist.invalid"));

    assertThrows(HopException.class, () -> http.callHttpPOST(new Object[0]));
  }

  @Test
  void callHttpPOSTResponseTimeAppearsInResultWhenConfigured() throws Exception {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    meta.getResultFields().get(0).setName("response");
    meta.getResultFields().get(0).setCode(null);
    meta.getResultFields().get(0).setResponseTimeFieldName("time");

    HttpPost http = newPost(meta, minimalData(baseUrl() + "/body"));
    Object[] result = http.callHttpPOST(new Object[0]);

    // result[0] = body, result[1] = response time
    assertTrue(result[1] instanceof Long);
    assertTrue((Long) result[1] >= 0L);
  }

  @Test
  void callHttpPOSTSetsDefaultContentTypeHeader() throws Exception {
    HttpPost http = newPost(bodyOnlyMeta(), minimalData(baseUrl() + "/echo"));
    http.callHttpPOST(new Object[0]);

    // addHeadersContentType sets text/xml when no encoding and no override
    assertNotNull(lastRequestContentType.get());
    assertTrue(lastRequestContentType.get().contains("text/xml"));
  }

  @Test
  void callHttpPOSTWithEncodingSetsContentTypeWithCharset() throws Exception {
    HttpPostMeta meta = bodyOnlyMeta();
    HttpPostData data = minimalData(baseUrl() + "/echo");
    data.realEncoding = "UTF-8";

    HttpPost http = newPost(meta, data);
    http.callHttpPOST(new Object[0]);

    assertNotNull(lastRequestContentType.get());
    assertTrue(lastRequestContentType.get().contains("UTF-8"));
  }

  // ---- reflection helpers ----

  private static Long getDataVolumeIn(HttpPost http) throws Exception {
    Field field = BaseTransform.class.getDeclaredField("dataVolumeIn");
    field.setAccessible(true);
    return (Long) field.get(http);
  }

  private static Long getDataVolumeOut(HttpPost http) throws Exception {
    Field field = BaseTransform.class.getDeclaredField("dataVolumeOut");
    field.setAccessible(true);
    return (Long) field.get(http);
  }
}
