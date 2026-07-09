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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class RestTest {

  private MockedStatic<Client> mockedClient;

  @BeforeEach
  void setUpStaticMocks() throws Exception {
    mockedClient = mockStatic(Client.class);
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  @AfterEach
  void tearDownStaticMocks() {
    mockedClient.closeOnDemand();
  }

  @Test
  @SuppressWarnings("unchecked")
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
    MultivaluedHashMap<String, String> map = rest.createMultivalueMap("param1", "{a:{[val1]}}");
    String val1 = map.getFirst("param1").toString();
    assertTrue(val1.contains("%7D"));
  }

  @Test
  void testSearchForHeaders() {
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

    Response response = mock(Response.class);
    MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add("Content-Type", "application/json");
    headers.add("X-Custom-Header", "custom-value");
    doReturn(headers).when(response).getHeaders();

    MultivaluedMap<String, Object> result = rest.searchForHeaders(response);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.containsKey("Content-Type"));
    assertTrue(result.containsKey("X-Custom-Header"));
  }

  @Test
  void testDispose() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestData data = new RestData();
    data.config = new org.glassfish.jersey.client.ClientConfig();
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
    assertNull(data.config);
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
  void testTrackResponseBytesFallsBackToBodyLength() throws Exception {
    Rest rest = newRest();
    Response response = mock(Response.class);
    doReturn(-1).when(response).getLength();
    doReturn(MediaType.valueOf("text/plain; charset=UTF-16LE")).when(response).getMediaType();

    invokePrivate(rest, "trackResponseBytes", response, "ok");

    assertEquals(4L, getLongField(rest, "dataVolumeIn"));
  }

  @Test
  void testConnectionApiKeyHeaderIsDecryptedAndRowWins() {
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

    Invocation.Builder invocationBuilder = mock(Invocation.Builder.class);

    // Fresh row: the connection contributes the decrypted, prefixed value.
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    connection.applyBearerAndApiKeyHeaders(invocationBuilder, headers);
    assertEquals("Token my_super_secret", headers.getFirst("X-API-Key"));

    // Row already supplied the header (case-insensitively): connection auth is skipped, row wins
    // and no second (differently-cased) copy is appended.
    MultivaluedMap<String, Object> rowHeaders = new MultivaluedHashMap<>();
    rowHeaders.putSingle("x-api-key", "row_value");
    connection.applyBearerAndApiKeyHeaders(invocationBuilder, rowHeaders);
    assertEquals(1, rowHeaders.get("x-api-key").size());
    assertEquals("row_value", rowHeaders.getFirst("x-api-key"));
    assertNull(rowHeaders.getFirst("X-API-Key"));
  }

  @Test
  void testConnectionApiKeyHeaderIsNotDuplicated() {
    // Regression for #6697: the connection's API-key header must be emitted exactly once — the
    // original bug sent it doubled (e.g. "my_super_secret,my_super_secret" -> HTTP 401). The test
    // button, RestConnection.getInvocationBuilder(...) and the transform all funnel auth through
    // this same merge, so re-applying it must stay idempotent instead of appending a second value.
    RestConnection connection = new RestConnection(new Variables());
    connection.setAuthType(RestConnection.API_KEY);
    connection.setAuthorizationHeaderName("X-API-Key");
    connection.setAuthorizationHeaderValue("my_super_secret");

    Invocation.Builder invocationBuilder = mock(Invocation.Builder.class);
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();

    connection.applyBearerAndApiKeyHeaders(invocationBuilder, headers);
    connection.applyBearerAndApiKeyHeaders(invocationBuilder, headers);

    assertEquals(1, headers.get("X-API-Key").size());
    assertEquals("my_super_secret", headers.getFirst("X-API-Key"));
  }

  private Rest newRest() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);
    return new Rest(
        transformMeta,
        mock(RestMeta.class),
        new RestData(),
        1,
        pipelineMeta,
        spy(new LocalPipelineEngine()));
  }

  private static Object invokePrivate(Object target, String methodName, Object... args)
      throws Exception {
    Method method =
        switch (methodName) {
          case "trackRequestBytes" ->
              target
                  .getClass()
                  .getDeclaredMethod(methodName, String.class, java.nio.charset.Charset.class);
          case "trackResponseBytes" ->
              target.getClass().getDeclaredMethod(methodName, Response.class, String.class);
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
