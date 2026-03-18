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
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
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
  void setUpStaticMocks() {
    mockedClient = mockStatic(Client.class);
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
  void testAddApiKeyHeaderIfAbsentAddsPrefixedHeaderWithoutOverriding() throws Exception {
    Rest rest = newRest();
    RestConnection connection = mock(RestConnection.class);
    doReturn("API Key").when(connection).getAuthType();
    doReturn("Authorization").when(connection).getAuthorizationHeaderName();
    doReturn("secret").when(connection).getAuthorizationHeaderValue();
    doReturn("Bearer").when(connection).getAuthorizationPrefix();

    Field connectionField = Rest.class.getDeclaredField("connection");
    connectionField.setAccessible(true);
    connectionField.set(rest, connection);

    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    invokePrivate(rest, "addApiKeyHeaderIfAbsent", headers);
    assertEquals("Bearer secret", headers.getFirst("Authorization"));

    headers.putSingle("Authorization", "existing");
    invokePrivate(rest, "addApiKeyHeaderIfAbsent", headers);
    assertEquals("existing", headers.getFirst("Authorization"));
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
          case "addApiKeyHeaderIfAbsent" ->
              target.getClass().getDeclaredMethod(methodName, MultivaluedMap.class);
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
