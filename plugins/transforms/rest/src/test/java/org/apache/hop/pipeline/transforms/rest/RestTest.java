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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.client.RestAuthenticator;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestTest {

  @BeforeEach
  void setUpEncryption() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @Test
  void testCreateMultivalueMap() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);
    Rest rest =
        new Rest(
            transformMeta,
            mock(RestMeta.class),
            mock(RestData.class),
            1,
            pipelineMeta,
            spy(new LocalPipelineEngine()));
    Map<String, String> map = rest.createMultivalueMap("param1", "{a:{[val1]}}");
    assertTrue(map.get("param1").contains("%7D"));
  }

  @Test
  void testDispose() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestData data = new RestData();
    data.headerNames = new String[] {"header1", "header2"};
    data.indexOfHeaderFields = new int[] {0, 1};
    data.paramNames = new String[] {"param1"};

    Rest rest =
        new Rest(
            transformMeta,
            mock(RestMeta.class),
            data,
            1,
            pipelineMeta,
            spy(new LocalPipelineEngine()));

    rest.dispose();

    // After dispose, these should be null
    assertNull(data.headerNames);
    assertNull(data.indexOfHeaderFields);
    assertNull(data.paramNames);
  }

  @Test
  void testTrackRequestBytesAddsBytesForCharset() throws Exception {
    Rest rest = newRest();

    invokePrivate(rest, "trackRequestBytes", "hello", StandardCharsets.UTF_16LE);

    assertEquals(10L, getLongField(rest, "dataVolumeOut"));
  }

  @Test
  void testResponseBytesAreCountedFromTheBody() throws Exception {
    // The response body is read into memory once, so its length is the byte count — no reliance on
    // a Content-Length header the server may not send.
    RestData data = new RestData();
    data.mediaType = ContentType.TEXT_PLAIN;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com";
    data.inputRowMeta = new RowMeta();
    data.client = FakeHttpClient.returning(200, "hello", Map.of());

    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());

    Rest rest = newRest(meta, data);
    rest.callRest(new Object[] {});

    assertEquals(5L, getLongField(rest, "dataVolumeIn"));
  }

  /** The authenticator a connection produces, as the transform would build it. */
  private static RestAuthenticator authenticatorFor(RestConnection connection) throws HopException {
    return new RestAuthenticator(connection.createClientSettings());
  }

  @Test
  void testConnectionApiKeyHeaderIsDecryptedAndRowWins() throws HopException {
    // Regression for #6697: an "Encrypted ..." API key configured on the REST connection must be
    // decrypted before it is sent (it used to be forwarded verbatim from the transform, giving a
    // 401), while a header already supplied on the incoming row must still win over connection
    // auth.
    String encryptedValue = Encr.encryptPasswordIfNotUsingVariables("my_super_secret");
    assertTrue(encryptedValue.startsWith(Encr.PASSWORD_ENCRYPTED_PREFIX));

    RestConnection connection = new RestConnection(new Variables());
    connection.setAuthType(RestConnection.API_KEY);
    connection.setAuthorizationHeaderName("X-API-Key");
    connection.setAuthorizationPrefix("Token");
    connection.setAuthorizationHeaderValue(encryptedValue);

    // Fresh row: the connection contributes the decrypted, prefixed value.
    Map<String, String> headers = new LinkedHashMap<>();
    authenticatorFor(connection).applyRequestHeaders(headers, "https://example.com/api");
    assertEquals("Token my_super_secret", headers.get("X-API-Key"));

    // Row already supplied the header (case-insensitively): connection auth is skipped, row wins
    // and no second (differently-cased) copy is appended.
    Map<String, String> rowHeaders = new LinkedHashMap<>();
    rowHeaders.put("x-api-key", "row_value");
    authenticatorFor(connection).applyRequestHeaders(rowHeaders, "https://example.com/api");
    assertEquals(1, rowHeaders.size());
    assertEquals("row_value", rowHeaders.get("x-api-key"));
    assertNull(rowHeaders.get("X-API-Key"));
  }

  @Test
  void testConnectionApiKeyHeaderIsNotDuplicated() throws HopException {
    // Regression for #6697: the connection's API-key header must be emitted exactly once — the
    // original bug sent it doubled (e.g. "my_super_secret,my_super_secret" -> HTTP 401). The test
    // button, the connection's own getResponse(...) and the transform all funnel auth through this
    // same authenticator, so re-applying it must stay idempotent instead of appending a second
    // value.
    RestConnection connection = new RestConnection(new Variables());
    connection.setAuthType(RestConnection.API_KEY);
    connection.setAuthorizationHeaderName("X-API-Key");
    connection.setAuthorizationHeaderValue("my_super_secret");

    Map<String, String> headers = new LinkedHashMap<>();
    RestAuthenticator authenticator = authenticatorFor(connection);

    authenticator.applyRequestHeaders(headers, "https://example.com/api");
    authenticator.applyRequestHeaders(headers, "https://example.com/api");

    assertEquals(1, headers.size());
    assertEquals("my_super_secret", headers.get("X-API-Key"));
  }

  private Rest newRest() {
    return newRest(mock(RestMeta.class), new RestData());
  }

  private Rest newRest(RestMeta meta, RestData data) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);
    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    return rest;
  }

  private static Object invokePrivate(Object target, String methodName, Object... args)
      throws Exception {
    Method method =
        switch (methodName) {
          case "trackRequestBytes" ->
              target
                  .getClass()
                  .getDeclaredMethod(methodName, String.class, java.nio.charset.Charset.class);
          default -> throw new NoSuchMethodException(methodName);
        };
    method.setAccessible(true);
    return method.invoke(target, args);
  }

  private static Long getLongField(Object target, String fieldName) throws Exception {
    Field field = BaseTransform.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Long) field.get(target);
  }
}
