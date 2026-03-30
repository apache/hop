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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.jupiter.api.Test;

class HttpTest {

  private Http newTransform(String encoding) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("HttpTest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("HttpTest");
    pipelineMeta.addTransform(transformMeta);

    HttpMeta meta = new HttpMeta();
    meta.setEncoding(encoding);

    HttpData data = new HttpData();
    data.realUrl = "http://localhost/test";

    return new Http(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
  }

  @Test
  void extractHeaderStringAggregatesDuplicateHeaders() throws Exception {
    Header[] headers = {
      new BasicHeader("X-Test", "first"),
      new BasicHeader("X-Test", "second"),
      new BasicHeader("Content-Type", "application/json")
    };

    JSONObject headerJson = (JSONObject) new JSONParser().parse(invokeExtractHeaderString(headers));

    assertEquals("application/json", headerJson.get("Content-Type"));
    assertTrue(headerJson.get("X-Test") instanceof JSONArray);
    assertEquals("first", ((JSONArray) headerJson.get("X-Test")).get(0));
    assertEquals("second", ((JSONArray) headerJson.get("X-Test")).get(1));
  }

  @Test
  void handResponseReadsBodyAndTracksDataVolumeIn() throws Exception {
    Http http = newTransform("UTF-8");
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    String payload = "hello http";
    doReturn(new StringEntity(payload, StandardCharsets.UTF_8)).when(response).getEntity();

    String body = invokeHandResponse(http, HttpURLConnection.HTTP_OK, response);

    assertEquals(payload, body);
    assertEquals(
        (long) payload.getBytes(StandardCharsets.UTF_8).length, getLongField(http, "dataVolumeIn"));
  }

  @Test
  void handResponseReturnsEmptyStringForNoContent() throws Exception {
    Http http = newTransform("UTF-8");
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);

    String body = invokeHandResponse(http, HttpURLConnection.HTTP_NO_CONTENT, response);

    assertEquals("", body);
  }

  @Test
  void handResponseThrowsForUnauthorizedStatus() {
    Http http = newTransform("UTF-8");
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);

    assertThrows(
        HopTransformException.class,
        () -> invokeHandResponse(http, HttpURLConnection.HTTP_UNAUTHORIZED, response));
  }

  private static String invokeHandResponse(
      Http http, int statusCode, CloseableHttpResponse response) throws Exception {
    Method method =
        http.getClass().getDeclaredMethod("handResponse", int.class, CloseableHttpResponse.class);
    method.setAccessible(true);
    try {
      return (String) method.invoke(http, statusCode, response);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof Exception exception) {
        throw exception;
      }
      throw e;
    }
  }

  private static String invokeExtractHeaderString(Header[] headers) throws Exception {
    Method method = Http.class.getDeclaredMethod("extractHeaderString", Header[].class);
    method.setAccessible(true);
    return (String) method.invoke(null, (Object) headers);
  }

  private static Long getLongField(Object target, String fieldName) throws Exception {
    Field field = BaseTransform.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Long) field.get(target);
  }
}
