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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
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
 * Tests for issue #3746: a binary request body or response must not be routed through a String.
 * Decoding bytes with a charset and re-encoding them replaces every byte that is not valid in that
 * charset with U+FFFD, which silently corrupts files, images and other non-text payloads.
 */
class RestBinaryBodyTest {

  /** A PNG signature plus bytes that are not valid UTF-8 - the shape of a real binary payload. */
  private static final byte[] BINARY = {
    (byte) 0x89,
    'P',
    'N',
    'G',
    0x0D,
    0x0A,
    0x1A,
    0x0A,
    (byte) 0xFF,
    (byte) 0xD8,
    (byte) 0xC3,
    (byte) 0x28,
    0x00,
    0x01
  };

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
  void testBinaryBodyFieldIsSentAsBytes() throws HopException {
    Invocation.Builder builder = mockBuilder(200, "");
    ClientBuilder clientBuilder = mockClientBuilder(builder);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_POST);
      meta.setUrl("http://example.com/api");
      meta.setBodyField("body");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE;
      data.method = RestMeta.HTTP_METHOD_POST;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useBody = true;
      data.indexOfBodyField = 1;
      data.binaryBody = true;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("field1"));
      inputRowMeta.addValueMeta(new ValueMetaBinary("body"));
      data.inputRowMeta = inputRowMeta;

      Rest rest = newRest(meta, data, clientBuilder);
      rest.callRest(new Object[] {"value1", BINARY});

      ArgumentCaptor<Entity> captor = ArgumentCaptor.forClass(Entity.class);
      verify(builder, times(1)).post(captor.capture());
      Object sent = captor.getValue().getEntity();
      assertInstanceOf(byte[].class, sent, "a binary body must not be converted to a String");
      assertArrayEquals(BINARY, (byte[]) sent);
    }
  }

  @Test
  void testStringBodyFieldStillGoesOutAsString() throws HopException {
    Invocation.Builder builder = mockBuilder(200, "");
    ClientBuilder clientBuilder = mockClientBuilder(builder);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_POST);
      meta.setUrl("http://example.com/api");
      meta.setBodyField("body");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_POST;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useBody = true;
      data.indexOfBodyField = 1;
      data.binaryBody = false;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("field1"));
      inputRowMeta.addValueMeta(new ValueMetaString("body"));
      data.inputRowMeta = inputRowMeta;

      Rest rest = newRest(meta, data, clientBuilder);
      rest.callRest(new Object[] {"value1", "{\"a\":1}"});

      ArgumentCaptor<Entity> captor = ArgumentCaptor.forClass(Entity.class);
      verify(builder, times(1)).post(captor.capture());
      assertEquals("{\"a\":1}", captor.getValue().getEntity());
    }
  }

  @Test
  void testBinaryResultFieldKeepsResponseBytes() throws HopException {
    Invocation.Builder builder = mockBuilder(200, null);
    Response response = builder.get(Response.class);
    when(response.hasEntity()).thenReturn(true);
    when(response.readEntity(byte[].class)).thenReturn(BINARY);
    ClientBuilder clientBuilder = mockClientBuilder(builder);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_GET);
      meta.setUrl("http://example.com/api");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");
      meta.getResultField().setBinary(true);

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE;
      data.method = RestMeta.HTTP_METHOD_GET;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.binaryResult = true;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("field1"));
      data.inputRowMeta = inputRowMeta;

      Rest rest = newRest(meta, data, clientBuilder);
      Object[] out = rest.callRest(new Object[] {"value1"});

      assertNotNull(out);
      assertInstanceOf(byte[].class, out[1], "a binary result must not be decoded to a String");
      assertArrayEquals(BINARY, (byte[]) out[1]);
    }
  }

  @Test
  void testGetFieldsDeclaresBinaryResultAsBinary() throws Exception {
    RestMeta meta = new RestMeta();
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(
        rowMeta, "REST", null, null, new org.apache.hop.core.variables.Variables(), null);
    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(0).getType());

    meta.getResultField().setBinary(true);
    IRowMeta binaryRowMeta = new RowMeta();
    meta.getFields(
        binaryRowMeta, "REST", null, null, new org.apache.hop.core.variables.Variables(), null);
    assertEquals(IValueMeta.TYPE_BINARY, binaryRowMeta.getValueMeta(0).getType());
  }

  /**
   * Locks in the connector behaviour both request paths rely on: a byte[] entity is written to the
   * wire untouched, whereas the old String entity was mangled by charset conversion.
   */
  @Test
  void testByteEntityReachesServerIntactOnBothConnectors() throws Exception {
    List<byte[]> received = new ArrayList<>();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/objects",
        exchange -> {
          received.add(exchange.getRequestBody().readAllBytes());
          exchange.sendResponseHeaders(200, BINARY.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(BINARY);
          }
        });
    server.start();
    String url = "http://localhost:" + server.getAddress().getPort() + "/objects";

    // Standalone path connector.
    ClientConfig apacheConfig = new ClientConfig();
    apacheConfig.connectorProvider(new Apache5ConnectorProvider());
    Client apache = ClientBuilder.newBuilder().withConfig(apacheConfig).build();
    try (Response r =
        apache
            .target(url)
            .request()
            .post(Entity.entity(BINARY, MediaType.APPLICATION_OCTET_STREAM_TYPE))) {
      assertArrayEquals(BINARY, r.readEntity(byte[].class), "response bytes must survive");
    } finally {
      apache.close();
    }

    // REST-connection path connector.
    Client jdk =
        ClientBuilder.newBuilder()
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .build();
    try (Response r =
        jdk.target(url)
            .request()
            .post(Entity.entity(BINARY, MediaType.APPLICATION_OCTET_STREAM_TYPE))) {
      assertArrayEquals(BINARY, r.readEntity(byte[].class), "response bytes must survive");
    } finally {
      jdk.close();
    }

    assertEquals(2, received.size());
    assertArrayEquals(BINARY, received.get(0), "Apache 5 connector corrupted the request body");
    assertArrayEquals(BINARY, received.get(1), "JDK connector corrupted the request body");
  }

  private Rest newRest(RestMeta meta, RestData data, ClientBuilder clientBuilder) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    Rest rest =
        spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
    when(rest.createClientBuilder()).thenReturn(clientBuilder);
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    return rest;
  }

  private Invocation.Builder mockBuilder(int status, String stringBody) {
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(status);
    if (stringBody != null) {
      when(response.readEntity(String.class)).thenReturn(stringBody);
    }
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add("Content-Type", "application/octet-stream");
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.post(any(Entity.class))).thenReturn(response);
    when(builder.get(Response.class)).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);
    return builder;
  }

  private ClientBuilder mockClientBuilder(Invocation.Builder builder) {
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
    return clientBuilder;
  }
}
