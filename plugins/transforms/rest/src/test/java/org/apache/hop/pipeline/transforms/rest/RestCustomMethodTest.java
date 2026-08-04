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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.glassfish.jersey.apache5.connector.Apache5ConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
  void testCallRestWithCustomMethodSendsMethodAndBody() throws HopException {
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("{\"items\":[]}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add("Content-Type", "application/json");
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.method(anyString(), any(Entity.class))).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

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
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = "LIST";
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useBody = true;
      data.indexOfBodyField = 1;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("field1"));
      inputRowMeta.addValueMeta(new ValueMetaString("body"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      Object[] inputRow = new Object[] {"value1", "{\"prefix\":\"a\"}"};
      Object[] outputRow = rest.callRest(inputRow);

      assertNotNull(outputRow);
      assertEquals("{\"items\":[]}", outputRow[2]);

      // The custom verb must reach the client verbatim, carrying the configured body.
      ArgumentCaptor<Entity> entityCaptor = ArgumentCaptor.forClass(Entity.class);
      verify(builder, times(1)).method(eq("LIST"), entityCaptor.capture());
      assertEquals("{\"prefix\":\"a\"}", entityCaptor.getValue().getEntity());
    }
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
  void testApache5ConnectorPutsCustomMethodOnTheWire() throws Exception {
    // Locks in the connector behaviour the fix relies on for the standalone (no REST connection)
    // path: Apache HttpClient 5 writes an arbitrary method token straight into the request line.
    String url = startEchoServer();

    ClientConfig config = new ClientConfig();
    config.connectorProvider(new Apache5ConnectorProvider());
    config.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
    Client client = ClientBuilder.newBuilder().withConfig(config).build();

    try (Response response =
        client
            .target(url)
            .request()
            .method("LIST", Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(200, response.getStatus());
      assertEquals("LIST", response.readEntity(String.class));
    } finally {
      client.close();
    }
  }

  @Test
  void testJdkConnectorPutsCustomMethodOnTheWire() throws Exception {
    // Same for the REST-connection path, which uses Jersey's default JDK HttpURLConnection
    // connector. HttpURLConnection.setRequestMethod() only accepts a fixed set of verbs, so this
    // relies on SET_METHOD_WORKAROUND reflecting into java.net.HttpURLConnection.method. That in
    // turn needs --add-opens java.base/java.net=ALL-UNNAMED, which the Hop launch scripts (and the
    // surefire argLine) pass. If this test starts failing with an InaccessibleObjectException, the
    // --add-opens has gone missing rather than the feature being wrong.
    String url = startEchoServer();

    Client client =
        ClientBuilder.newBuilder()
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true)
            .build();

    try (Response response =
        client
            .target(url)
            .request()
            .method("LIST", Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE))) {
      assertEquals(200, response.getStatus());
      assertEquals("LIST", response.readEntity(String.class));
    } finally {
      client.close();
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
