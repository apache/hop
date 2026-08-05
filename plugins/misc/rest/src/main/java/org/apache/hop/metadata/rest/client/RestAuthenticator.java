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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hop.core.util.Utils;

/**
 * Applies authentication to a REST request. One class for all four schemes, whether the settings
 * came from a REST connection or from the REST transform's own fields.
 *
 * <p>Credentials are bound to an origin. A REST transform can take its URL from an input field, so
 * consecutive rows may target different hosts on one client; without an origin check, credentials
 * configured for the connection's own host would be handed to whatever host a row happened to name.
 * When no origin can be determined — a URL field with no base URL to anchor it — the credentials
 * are sent unconditionally, which is what this transform has always done.
 */
public class RestAuthenticator {

  private final RestClientSettings settings;

  public RestAuthenticator(RestClientSettings settings) {
    this.settings = settings;
  }

  /**
   * Adds the authentication headers for one request. Headers already on the map win: a row that
   * supplies its own {@code Authorization} (or the configured API-key header) keeps it.
   *
   * @param headers the outbound header map, modified in place
   * @param requestUri the URI this request is going to
   */
  public void applyRequestHeaders(Map<String, String> headers, String requestUri) {
    if (headers == null || !targetMatchesOrigin(requestUri)) {
      return;
    }
    switch (settings.getAuthType()) {
      case BASIC -> applyBasic(headers);
      case BEARER -> applyBearer(headers);
      case API_KEY -> applyApiKey(headers);
      case NONE -> {
        // Nothing to add.
      }
    }
  }

  /**
   * The credentials provider answering challenges, or {@code null} when nothing here needs one. Two
   * kinds of challenge end up in it: a 401 from the target when Basic authentication is not
   * preemptive, and a 407 from an authenticating proxy. Preemptive Basic needs no provider — the
   * header is written up front by {@link #applyRequestHeaders}.
   */
  public CredentialsProvider createCredentialsProvider() {
    boolean challengeResponseBasic =
        settings.getAuthType() == RestAuthType.BASIC
            && !settings.isBasicPreemptive()
            && hasBasicCredentials();
    HttpHost proxy = RestProxyRoutePlanner.proxyOf(settings);
    boolean proxyAuth = proxy != null && !Utils.isEmpty(settings.getProxyUsername());

    if (!challengeResponseBasic && !proxyAuth) {
      return null;
    }

    BasicCredentialsProvider provider = new BasicCredentialsProvider();
    if (challengeResponseBasic) {
      provider.setCredentials(
          authScope(), credentials(settings.getBasicUsername(), settings.getBasicPassword()));
    }
    if (proxyAuth) {
      provider.setCredentials(
          new AuthScope(proxy),
          credentials(settings.getProxyUsername(), settings.getProxyPassword()));
    }
    return provider;
  }

  private static UsernamePasswordCredentials credentials(String username, String password) {
    return new UsernamePasswordCredentials(
        Utils.isEmpty(username) ? "" : username, (password == null ? "" : password).toCharArray());
  }

  private void applyBasic(Map<String, String> headers) {
    // Only preemptive Basic writes a header. The challenge-response variant waits for the 401 and
    // is answered by the credentials provider on the client.
    if (!settings.isBasicPreemptive()
        || !hasBasicCredentials()
        || headerAlreadySupplied(headers, HttpHeaders.AUTHORIZATION)) {
      return;
    }
    String credentials =
        Utils.isEmpty(settings.getBasicUsername()) ? "" : settings.getBasicUsername();
    credentials += ":" + (settings.getBasicPassword() == null ? "" : settings.getBasicPassword());
    headers.put(
        HttpHeaders.AUTHORIZATION,
        "Basic "
            + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
  }

  private void applyBearer(Map<String, String> headers) {
    if (Utils.isEmpty(settings.getBearerToken())
        || headerAlreadySupplied(headers, HttpHeaders.AUTHORIZATION)) {
      return;
    }
    headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + settings.getBearerToken());
  }

  private void applyApiKey(Map<String, String> headers) {
    String name = settings.getApiKeyHeaderName();
    String value = settings.getApiKeyHeaderValue();
    if (Utils.isEmpty(name) || Utils.isEmpty(value) || headerAlreadySupplied(headers, name)) {
      return;
    }
    String prefix = settings.getApiKeyHeaderPrefix();
    headers.put(name, Utils.isEmpty(prefix) ? value : prefix + " " + value);
  }

  private boolean hasBasicCredentials() {
    return !Utils.isEmpty(settings.getBasicUsername())
        || !Utils.isEmpty(settings.getBasicPassword());
  }

  /** True when the row did not already supply this header with a value of its own. */
  private static boolean headerAlreadySupplied(Map<String, String> headers, String name) {
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
        String value = entry.getValue();
        if (value != null && !Utils.isEmpty(value.trim())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * True when the request is going to the host the credentials were configured for, or when no
   * origin is known and the check therefore cannot be made.
   */
  private boolean targetMatchesOrigin(String requestUri) {
    HttpHost origin = originHost();
    if (origin == null) {
      return true;
    }
    HttpHost target = hostOf(requestUri);
    return target != null
        && origin.getSchemeName().equalsIgnoreCase(target.getSchemeName())
        && origin.getHostName().equalsIgnoreCase(target.getHostName())
        && origin.getPort() == target.getPort();
  }

  private AuthScope authScope() {
    HttpHost origin = originHost();
    return origin == null ? new AuthScope(null, null, -1, null, null) : new AuthScope(origin);
  }

  private HttpHost originHost() {
    return hostOf(settings.getAuthOrigin());
  }

  /** The scheme, host and port of a URL, or {@code null} when it has none or cannot be parsed. */
  private static HttpHost hostOf(String url) {
    if (Utils.isEmpty(url)) {
      return null;
    }
    try {
      URI uri = URI.create(url.trim());
      if (uri.getScheme() == null || uri.getHost() == null) {
        return null;
      }
      return new HttpHost(uri.getScheme(), uri.getHost(), uri.getPort());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
