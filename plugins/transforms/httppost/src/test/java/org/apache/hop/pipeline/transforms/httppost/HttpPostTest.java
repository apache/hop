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
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hop.core.Const;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class HttpPostTest {

  private HttpPost newTransform() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("HttpPostTest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("HttpPostTest");
    pipelineMeta.addTransform(transformMeta);
    return new HttpPost(
        transformMeta,
        new HttpPostMeta(),
        new HttpPostData(),
        0,
        pipelineMeta,
        new LocalPipelineEngine());
  }

  @Test
  void getRequestBodyParametersAsStringWithNullEncoding() {
    HttpPost http = newTransform();

    NameValuePair[] pairs =
        new NameValuePair[] {
          new BasicNameValuePair("u", "usr"), new BasicNameValuePair("p", "pass")
        };

    assertEquals("u=usr&p=pass", http.getRequestBodyParamsAsStr(pairs, null));
  }

  @Test
  void getRequestBodyParametersAsStringWithEncodingEscapesContent() {
    HttpPost http = newTransform();

    NameValuePair[] pairs = {
      new BasicNameValuePair("city", "Sao Paulo"), new BasicNameValuePair("q", "a+b")
    };

    assertEquals("city=Sao+Paulo&q=a%2Bb", http.getRequestBodyParamsAsStr(pairs, Const.UTF_8));
  }

  @Test
  void attachRawRequestEntityIfNeededSetsEntityWhenMissing() {
    HttpPost http = newTransform();

    org.apache.hc.client5.http.classic.methods.HttpPost post =
        new org.apache.hc.client5.http.classic.methods.HttpPost("http://localhost");

    http.attachRawRequestEntityIfNeeded(post, "payload".getBytes());

    assertEquals(7, post.getEntity().getContentLength());
  }

  @Test
  void attachRawRequestEntityIfNeededDoesNotOverrideExistingEntity() throws Exception {
    HttpPost http = newTransform();

    org.apache.hc.client5.http.classic.methods.HttpPost post =
        new org.apache.hc.client5.http.classic.methods.HttpPost("http://localhost");
    org.apache.hc.core5.http.HttpEntity existing = new StringEntity("existing");
    post.setEntity(existing);

    http.attachRawRequestEntityIfNeeded(post, "payload".getBytes());

    assertSame(existing, post.getEntity());
  }

  @Test
  void collectRequestBytesCountsUnknownLengthEntity() {
    HttpPost http = newTransform();

    org.apache.hc.core5.http.HttpEntity entity =
        new InputStreamEntity(new ByteArrayInputStream("payload".getBytes()), -1, null);

    http.collectRequestBytes(entity);

    assertEquals(7L, http.getTrackedDataVolumeOut());
  }

  @Test
  void collectRequestBytesCountsKnownLengthEntity() {
    HttpPost http = newTransform();

    org.apache.hc.core5.http.HttpEntity entity =
        new StringEntity("payload", StandardCharsets.UTF_8);

    http.collectRequestBytes(entity);

    assertEquals(7L, http.getTrackedDataVolumeOut());
  }

  @Test
  void readResponseBodyTracksDataVolumeIn() throws Exception {
    HttpPost http = newTransform();

    String body =
        (String)
            invokePrivate(
                http, "readResponseBody", new StringEntity("response", StandardCharsets.UTF_8));

    assertEquals("response", body);
    assertEquals(8L, getLongField(http, "dataVolumeIn"));
  }

  @Test
  void openStreamWithEncodingReturnsReaderDecodedInThatEncoding() throws Exception {
    HttpPost http = newTransform();
    CloseableHttpResponse response = responseWithBody("hello");

    try (InputStreamReader reader = http.openStream(Const.UTF_8, response)) {
      char[] buf = new char[5];
      int read = reader.read(buf);
      assertEquals(5, read);
      assertEquals("hello", new String(buf));
    }
  }

  @Test
  void openStreamWithoutEncodingReturnsDefaultReader() throws Exception {
    HttpPost http = newTransform();
    CloseableHttpResponse response = responseWithBody("world");

    try (InputStreamReader reader = http.openStream(null, response)) {
      char[] buf = new char[5];
      int read = reader.read(buf);
      assertEquals(5, read);
      assertEquals("world", new String(buf));
    }
  }

  @Test
  void openStreamWithEmptyEncodingReturnsDefaultReader() throws Exception {
    HttpPost http = newTransform();
    CloseableHttpResponse response = responseWithBody("hop");

    try (InputStreamReader reader = http.openStream("", response)) {
      char[] buf = new char[3];
      int read = reader.read(buf);
      assertEquals(3, read);
      assertEquals("hop", new String(buf));
    }
  }

  private static CloseableHttpResponse responseWithBody(String body) {
    CloseableHttpResponse response = org.mockito.Mockito.mock(CloseableHttpResponse.class);
    org.mockito.Mockito.when(response.getEntity())
        .thenReturn(new StringEntity(body, StandardCharsets.UTF_8));
    return response;
  }

  private static Object invokePrivate(Object target, String methodName, Object... args)
      throws Exception {
    Method method =
        target.getClass().getDeclaredMethod(methodName, org.apache.hc.core5.http.HttpEntity.class);
    method.setAccessible(true);
    return method.invoke(target, args);
  }

  private static Long getLongField(Object target, String fieldName) throws Exception {
    Field field = BaseTransform.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Long) field.get(target);
  }
}
