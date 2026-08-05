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
import static org.mockito.Mockito.mock;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
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
  void testBinaryBodyFieldIsSentAsBytes() throws Exception {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_POST);
    meta.setUrl("http://example.com/api");
    meta.setBodyField("body");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_OCTET_STREAM;
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

    Rest rest = newRest(meta, data, octetStream(200, new byte[0]));
    rest.callRest(new Object[] {"value1", BINARY});

    // Byte-for-byte: a trip through a String would have replaced the invalid UTF-8 with U+FFFD.
    assertArrayEquals(BINARY, EntityUtils.toByteArray(FakeHttpClient.captured().getEntity()));
  }

  @Test
  void testStringBodyFieldStillGoesOutAsString() throws Exception {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_POST);
    meta.setUrl("http://example.com/api");
    meta.setBodyField("body");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
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

    Rest rest = newRest(meta, data, octetStream(200, new byte[0]));
    rest.callRest(new Object[] {"value1", "{\"a\":1}"});

    assertEquals(
        "{\"a\":1}",
        EntityUtils.toString(FakeHttpClient.captured().getEntity(), StandardCharsets.UTF_8));
  }

  @Test
  void testBinaryResultFieldKeepsResponseBytes() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/api");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setBinary(true);

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_OCTET_STREAM;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.binaryResult = true;

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = inputRowMeta;

    Rest rest = newRest(meta, data, octetStream(200, BINARY));
    Object[] out = rest.callRest(new Object[] {"value1"});

    assertNotNull(out);
    assertInstanceOf(byte[].class, out[1], "a binary result must not be decoded to a String");
    assertArrayEquals(BINARY, (byte[]) out[1]);
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
   * Locks in the behaviour the transform relies on against a real server: a byte[] entity is
   * written to the wire untouched, and the response bytes come back the same way. The old String
   * entity was mangled by charset conversion in both directions.
   */
  @Test
  void testByteEntityReachesServerIntact() throws Exception {
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

    org.apache.hc.client5.http.classic.methods.HttpPost post =
        new org.apache.hc.client5.http.classic.methods.HttpPost(url);
    post.setEntity(new ByteArrayEntity(BINARY, ContentType.APPLICATION_OCTET_STREAM));

    try (CloseableHttpClient client = RestClientFactory.createClient(new RestClientSettings())) {
      byte[] responseBody =
          client.execute(post, response -> EntityUtils.toByteArray(response.getEntity()));
      assertArrayEquals(BINARY, responseBody, "response bytes must survive");
    }

    assertEquals(1, received.size());
    assertArrayEquals(BINARY, received.get(0), "the request body was corrupted");
  }

  private Rest newRest(RestMeta meta, RestData data, CloseableHttpClient client) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    data.client = client;

    Rest rest = new Rest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    return rest;
  }

  private static CloseableHttpClient octetStream(int status, byte[] body) {
    return FakeHttpClient.returning(
        status, body, Map.of("Content-Type", "application/octet-stream"));
  }
}
