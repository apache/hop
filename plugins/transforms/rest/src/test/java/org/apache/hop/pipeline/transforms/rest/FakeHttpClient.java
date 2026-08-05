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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.message.BasicHeader;

/**
 * A stand-in {@link CloseableHttpClient} for transform tests.
 *
 * <p>The transform reads the response inside the {@code execute(request, handler)} callback,
 * because HttpClient5 releases the connection as soon as the exchange returns. Mocking that shape
 * once here keeps the tests from re-deriving it: {@link #captured()} exposes the request that was
 * sent, so assertions about method, URL, headers and body stay straightforward.
 */
public final class FakeHttpClient {

  private FakeHttpClient() {
    // Factory class
  }

  /** A client that answers every request with this status, body and headers. */
  public static CloseableHttpClient returning(
      int status, String body, Map<String, String> responseHeaders) {
    return returning(status, body.getBytes(StandardCharsets.UTF_8), responseHeaders);
  }

  /** A client that answers every request with this status, raw body and headers. */
  public static CloseableHttpClient returning(
      int status, byte[] body, Map<String, String> responseHeaders) {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    try {
      when(client.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
          .thenAnswer(
              invocation -> {
                CAPTURED.set(invocation.getArgument(0));
                HttpClientResponseHandler<?> handler = invocation.getArgument(1);
                return handler.handleResponse(response(status, body, responseHeaders));
              });
    } catch (Exception e) {
      throw new IllegalStateException("Unable to stub the HTTP client", e);
    }
    return client;
  }

  /** The most recent request the transform issued, for assertions. */
  public static ClassicHttpRequest captured() {
    return CAPTURED.get();
  }

  private static final ThreadLocal<ClassicHttpRequest> CAPTURED = new ThreadLocal<>();

  private static ClassicHttpResponse response(
      int status, byte[] body, Map<String, String> responseHeaders) {
    ClassicHttpResponse response = mock(ClassicHttpResponse.class);
    when(response.getCode()).thenReturn(status);

    Map<String, String> headers = responseHeaders == null ? new LinkedHashMap<>() : responseHeaders;
    List<Header> headerList = new ArrayList<>();
    headers.forEach((name, value) -> headerList.add(new BasicHeader(name, value)));
    when(response.getHeaders()).thenReturn(headerList.toArray(new Header[0]));

    String contentType = headers.getOrDefault("Content-Type", ContentType.TEXT_PLAIN.getMimeType());
    when(response.getEntity())
        .thenReturn(new ByteArrayEntity(body, ContentType.parse(contentType)));
    return response;
  }
}
