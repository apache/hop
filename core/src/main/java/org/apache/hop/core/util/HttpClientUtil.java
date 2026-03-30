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

package org.apache.hop.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.auth.AuthCache;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicAuthCache;
import org.apache.hc.client5.http.impl.auth.BasicScheme;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

/**
 * Utility class contained useful methods while working with {@link
 * org.apache.hc.client5.http.classic.HttpClient HttpClient}
 */
public class HttpClientUtil {

  private HttpClientUtil() {}

  /**
   * @param response the httpresponse for processing
   * @return HttpEntity in String representation using "UTF-8" encoding
   * @throws IOException
   */
  public static String responseToString(ClassicHttpResponse response) throws IOException {
    return responseToString(response, Charset.forName(StandardCharsets.UTF_8.name()));
  }

  /**
   * @param response the httpresponse for processing
   * @param charset the charset used for getting HttpEntity
   * @return HttpEntity in decoded String representation using provided charset
   * @throws IOException
   */
  public static String responseToString(ClassicHttpResponse response, Charset charset)
      throws IOException {
    return responseToString(response, charset, false);
  }

  /**
   * @param response the httpresponse for processing
   * @param charset the charset used for getting HttpEntity
   * @param decode determines if the result should be decoded or not
   * @return HttpEntity in String representation using provided charset
   * @throws IOException
   */
  public static String responseToString(
      ClassicHttpResponse response, Charset charset, boolean decode) throws IOException {
    HttpEntity entity = response.getEntity();
    String result;
    try {
      result = EntityUtils.toString(entity, charset);
    } catch (ParseException e) {
      throw new IOException("Unable to parse HTTP response entity", e);
    }
    EntityUtils.consume(entity);
    if (decode) {
      result = URLDecoder.decode(result, StandardCharsets.UTF_8.name());
    }
    return result;
  }

  public static InputStream responseToInputStream(ClassicHttpResponse response) throws IOException {
    return response.getEntity().getContent();
  }

  public static byte[] responseToByteArray(ClassicHttpResponse response) throws IOException {
    return EntityUtils.toByteArray(response.getEntity());
  }

  /**
   * Returns context with AuthCache or null in case of any exception was thrown.
   *
   * @param host
   * @param port
   * @param user
   * @param password
   * @param schema
   * @return {@link org.apache.hc.client5.http.protocol.HttpClientContext HttpClientContext}
   */
  public static HttpClientContext createPreemptiveBasicAuthentication(
      String host, int port, String user, String password, String schema) {
    HttpClientContext localContext = null;
    try {
      HttpHost target = new HttpHost(schema, host, port);
      char[] passwordChars = password != null ? password.toCharArray() : new char[0];
      AuthCache authCache = new BasicAuthCache();
      BasicScheme basicAuth = new BasicScheme();
      basicAuth.initPreemptive(new UsernamePasswordCredentials(user, passwordChars));
      authCache.put(target, basicAuth);

      // Add AuthCache to the execution context
      localContext = HttpClientContext.create();
      localContext.setAuthCache(authCache);
    } catch (Exception e) {
      return null;
    }
    return localContext;
  }

  /**
   * Returns context with AuthCache or null in case of any exception was thrown. Use "http" schema.
   *
   * @param host
   * @param port
   * @param user
   * @param password
   * @return {@link org.apache.hc.client5.http.protocol.HttpClientContext HttpClientContext}
   */
  public static HttpClientContext createPreemptiveBasicAuthentication(
      String host, int port, String user, String password) {
    return createPreemptiveBasicAuthentication(host, port, user, password, "http");
  }
}
