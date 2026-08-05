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

import javax.net.ssl.SSLContext;
import lombok.Getter;
import lombok.Setter;

/**
 * Everything needed to build an HTTP client, fully resolved: no variables, no encrypted passwords,
 * no Hop metadata.
 *
 * <p>This is the single model behind both ways of configuring a REST call — a {@link
 * org.apache.hop.metadata.rest.RestConnection} or the REST transform's own fields. Each source
 * populates one of these, {@link RestClientFactory} turns it into a client, and no code downstream
 * needs to know which source it came from.
 *
 * <p>Values that vary per request (URL, method, headers, query and matrix parameters, body) are
 * deliberately absent: they belong to the request, not to the client.
 */
@Getter
@Setter
public class RestClientSettings {

  /** Connect timeout in milliseconds. {@code null} leaves Jersey's default (0 = infinite). */
  private Integer connectTimeout;

  /** Read timeout in milliseconds. {@code null} leaves Jersey's default (0 = infinite). */
  private Integer readTimeout;

  /**
   * Scheme used to reach the proxy itself, not the target: {@code http} (default) or {@code https}.
   */
  private String proxyScheme;

  private String proxyHost;
  private Integer proxyPort;
  private String proxyUsername;

  /** Plain-text proxy password: decrypt before setting this. */
  private String proxyPassword;

  /**
   * Hosts that bypass the proxy, in JDK {@code http.nonProxyHosts} syntax: entries separated by
   * {@code |} (commas and semicolons are accepted too), each optionally using {@code *} as a
   * wildcard, e.g. {@code localhost|127.*|*.internal.example.com}.
   */
  private String nonProxyHosts;

  /** Pre-built SSL context, or {@code null} to use the JVM default. */
  private SSLContext sslContext;

  /**
   * Accept any host name on the server certificate. Goes with a trust-all or custom trust store.
   */
  private boolean permissiveHostnameVerifier;

  /** Which authentication scheme applies. Never {@code null}. */
  private RestAuthType authType = RestAuthType.NONE;

  /**
   * The {@code scheme://host[:port]} the credentials belong to. Requests to any other host go out
   * unauthenticated. {@code null} disables the check, which is the only option when the URL comes
   * from an input field with no base URL to anchor it.
   */
  private String authOrigin;

  private String basicUsername;

  /** Plain-text password: decrypt before setting this. */
  private String basicPassword;

  /**
   * Send Basic credentials on the first request rather than waiting for a 401 challenge. This is
   * what both REST paths have always done, so it stays the default.
   */
  private boolean basicPreemptive = true;

  /** Plain-text bearer token: decrypt before setting this. */
  private String bearerToken;

  private String apiKeyHeaderName;
  private String apiKeyHeaderPrefix;

  /** Plain-text API key: decrypt before setting this. */
  private String apiKeyHeaderValue;
}
