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

package org.apache.hop.core;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hc.client5.http.auth.AuthenticationException;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.HttpClientUtil;
import org.apache.hop.core.util.Utils;

/**
 * HTTP
 *
 * <p>This class contains HTTP protocol properties such as request headers. Response headers and
 * other properties of the HTTP protocol can be added to this class.
 */
public class HttpProtocol {

  /*
   * Array of HTTP request headers- this list is incomplete and more headers can be added as needed.
   */

  private static final String[] requestHeaders = {
    "accept", "accept-charset", "cache-control", "content-type"
  };

  /**
   * @return array of HTTP request headers
   */
  public static String[] getRequestHeaders() {
    return requestHeaders;
  }

  /**
   * Performs a get on urlAsString using username and password as credentials.
   *
   * <p>If the status code returned not -1 and 401 then the contents are returned. If the status
   * code is 401 an AuthenticationException is thrown.
   *
   * <p>All other values of status code are not dealt with but logic can be added as needed.
   *
   * @param urlAsString
   * @param username
   * @param password
   * @return
   * @throws AuthenticationException
   * @throws IOException
   */
  public String get(String urlAsString, String username, String password)
      throws IOException, AuthenticationException {

    HttpClient httpClient;
    HttpGet getMethod = new HttpGet(urlAsString);
    HttpClientContext clientContext = HttpClientContext.create();
    if (!Utils.isEmpty(username)) {
      HttpClientManager.HttpClientBuilderFacade clientBuilder =
          HttpClientManager.getInstance().createBuilder();
      clientBuilder.setCredentials(username, password);
      httpClient = clientBuilder.build();
      HttpHost origin = HttpHost.create(URI.create(urlAsString));
      HttpClientContext preemptive =
          HttpClientUtil.createPreemptiveBasicAuthentication(
              origin.getHostName(), origin.getPort(), username, password, origin.getSchemeName());
      if (preemptive != null) {
        clientContext = preemptive;
      }
    } else {
      httpClient = HttpClientManager.getInstance().createDefaultClient();
    }
    ClassicHttpResponse httpResponse =
        (ClassicHttpResponse) httpClient.execute(getMethod, clientContext);
    int statusCode = httpResponse.getCode();
    StringBuilder bodyBuffer = new StringBuilder();

    if (statusCode != -1) {
      if (statusCode != HttpStatus.SC_UNAUTHORIZED) {
        // the response
        InputStreamReader inputStreamReader =
            new InputStreamReader(httpResponse.getEntity().getContent());

        int c;
        while ((c = inputStreamReader.read()) != -1) {
          bodyBuffer.append((char) c);
        }
        inputStreamReader.close();

      } else {
        throw new AuthenticationException();
      }
    }

    // Display response
    return bodyBuffer.toString();
  }
}
