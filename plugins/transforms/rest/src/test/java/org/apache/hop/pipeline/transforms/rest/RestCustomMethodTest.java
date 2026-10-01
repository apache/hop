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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.rest.client.RestClientFactory;
import org.apache.hop.metadata.rest.client.RestClientSettings;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for issue #4770: the REST transform must be able to send a custom HTTP method such as
 * {@code LIST} or {@code PURGE}, instead of rejecting anything outside the seven well-known verbs.
 */
class RestCustomMethodTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private HttpServer server;

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @BeforeEach
  void setUp() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }

  @Test
  void testCallRestWithCustomMethodSendsMethodAndBody() throws Exception {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod("LIST");
    meta.setUrl("http://example.com/api");
    meta.setBodyField("body");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = "LIST";
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.useBody = true;
    data.indexOfBodyField = 1;
    data.client =
        FakeHttpClient.returning(200, "{\"items\":[]}", Map.of("Content-Type", "application/json"));

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));
    inputRowMeta.addValueMeta(new ValueMetaString("body"));
    data.inputRowMeta = inputRowMeta;

    Rest rest = new Rest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    Object[] outputRow = rest.callRest(new Object[] {"value1", "{\"prefix\":\"a\"}"});

    assertNotNull(outputRow);
    assertEquals("{\"items\":[]}", outputRow[2]);

    // The custom verb must reach the request line verbatim, carrying the configured body.
    assertEquals("LIST", FakeHttpClient.captured().getMethod());
    assertEquals(
        "{\"prefix\":\"a\"}",
        EntityUtils.toString(FakeHttpClient.captured().getEntity(), StandardCharsets.UTF_8));
  }

  @Test
  void testInvalidMethodTokenIsRejected() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setDynamicMethod(true);
    meta.setMethodFieldName("method");

    RestData data = new RestData();
    data.indexOfMethod = 0;
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("method"));
    data.inputRowMeta = inputRowMeta;

    Rest rest =
        new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine()));

    // A method arriving from an input field must not be able to inject a request line.
    HopException e =
        assertThrows(
            HopException.class,
            () -> rest.applyDynamicRowUrlAndMethod(new Object[] {"GET /admin HTTP/1.1\r\nX: 1"}));
    assertTrue(e.getMessage().contains("not a valid HTTP method token"), e.getMessage());
  }

  @Test
  void testFactoryClientPutsCustomMethodOnTheWire() throws Exception {
    // Locks in the client behaviour the fix relies on, now for both paths at once: every client
    // the factory produces uses Apache HttpClient 5, which writes an arbitrary method token
    // straight into the request line. The REST connection used to run on Jersey's default JDK
    // connector, where the same verb only worked by reflecting into
    // java.net.HttpURLConnection.method via SET_METHOD_WORKAROUND.
    String url = startEchoServer();

    BasicClassicHttpRequest request = new BasicClassicHttpRequest("LIST", url);
    request.setEntity(new StringEntity("{}", ContentType.APPLICATION_JSON));

    try (CloseableHttpClient client = RestClientFactory.createClient(new RestClientSettings())) {
      String echoedMethod =
          client.execute(
              request,
              response -> {
                assertEquals(200, response.getCode());
                return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
              });
      assertEquals("LIST", echoedMethod);
    }
  }

  /** Starts a server that echoes back the method it saw, and returns its URL. */
  private String startEchoServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/objects",
        exchange -> {
          try (var ignored = exchange.getRequestBody()) {
            exchange.getRequestBody().readAllBytes();
          }
          byte[] body = exchange.getRequestMethod().getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
    return "http://localhost:" + server.getAddress().getPort() + "/objects";
  }
}
