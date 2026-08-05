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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hc.client5.http.impl.routing.DefaultRoutePlanner;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hop.core.util.Utils;

/**
 * Decides per request whether to go through the configured proxy.
 *
 * <p>The decision has to be made per route rather than once per client, because the bypass list is
 * a property of the target rather than of the client — and a REST transform can take its URL from
 * an input field, so one client serves a target host that changes from row to row. Jersey's {@code
 * ClientProperties.PROXY_URI} ends in {@code HttpClientBuilder.setProxy()}, whose {@code
 * DefaultProxyRoutePlanner} returns the proxy unconditionally, with nowhere to express a bypass.
 */
public class RestProxyRoutePlanner extends DefaultRoutePlanner {

  /** Separators accepted in the bypass list: the JDK uses {@code |}, we also allow , and ;. */
  private static final Pattern SEPARATOR = Pattern.compile("[|,;]");

  private static final String DEFAULT_SCHEME = "http";
  private static final int DEFAULT_HTTP_PORT = 8080;
  private static final int DEFAULT_HTTPS_PORT = 443;

  private final HttpHost proxy;
  private final List<Pattern> bypassPatterns;

  public RestProxyRoutePlanner(RestClientSettings settings) {
    super(null);
    this.proxy = proxyOf(settings);
    this.bypassPatterns = compileBypassPatterns(settings.getNonProxyHosts());
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

  @Override
  protected HttpHost determineProxy(HttpHost target, HttpContext context) {
    if (proxy == null || bypasses(target.getHostName())) {
      // A null proxy means a direct route.
      return null;
    }
    return proxy;
  }

  /** True when this host is on the bypass list and must be reached directly. */
  public boolean bypasses(String hostName) {
    if (hostName == null || bypassPatterns.isEmpty()) {
      return false;
    }
    for (Pattern pattern : bypassPatterns) {
      if (pattern.matcher(hostName).matches()) {
        return true;
      }
    }
    return false;
  }

  private static List<Pattern> compileBypassPatterns(String nonProxyHosts) {
    List<Pattern> patterns = new ArrayList<>();
    if (Utils.isEmpty(nonProxyHosts)) {
      return patterns;
    }
    for (String entry : SEPARATOR.split(nonProxyHosts)) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      // Quote everything, then re-open the * wildcards, so a host like "10.0.0.1" cannot be read
      // as a regular expression of its own.
      String[] literals = trimmed.split("\\*", -1);
      StringBuilder regex = new StringBuilder();
      for (int i = 0; i < literals.length; i++) {
        // Driven by the index, not by what has been appended so far: a leading wildcard leaves an
        // empty first literal, and "*.internal" has to keep its wildcard.
        if (i > 0) {
          regex.append(".*");
        }
        if (!literals[i].isEmpty()) {
          regex.append(Pattern.quote(literals[i]));
        }
      }
      patterns.add(Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE));
    }
    return patterns;
  }
}
