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

package org.apache.hop.metadata.rest.client;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hop.core.util.ProxyRoutePlanner;
import org.apache.hop.core.util.Utils;

/**
 * A {@link ProxyRoutePlanner} configured from a REST connection's settings: which proxy to use, and
 * which target hosts bypass it.
 */
public class RestProxyRoutePlanner extends ProxyRoutePlanner {

  private static final String DEFAULT_SCHEME = "http";
  private static final int DEFAULT_HTTP_PORT = 8080;
  private static final int DEFAULT_HTTPS_PORT = 443;

  public RestProxyRoutePlanner(RestClientSettings settings) {
    super(proxyOf(settings), settings.getNonProxyHosts());
  }

  /** The proxy described by the settings, or {@code null} when none is configured. */
  public static HttpHost proxyOf(RestClientSettings settings) {
    if (Utils.isEmpty(settings.getProxyHost())) {
      return null;
    }
    String scheme =
        Utils.isEmpty(settings.getProxyScheme())
            ? DEFAULT_SCHEME
            : settings.getProxyScheme().trim();
    int port =
        settings.getProxyPort() != null
            ? settings.getProxyPort()
            : ("https".equalsIgnoreCase(scheme) ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT);
    return new HttpHost(scheme, settings.getProxyHost().trim(), port);
  }
}
