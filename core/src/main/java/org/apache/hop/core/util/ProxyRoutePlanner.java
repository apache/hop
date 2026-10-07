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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hc.client5.http.impl.routing.DefaultRoutePlanner;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.protocol.HttpContext;

/**
 * Decides per request whether to go through the configured proxy.
 *
 * <p>The decision has to be made per route rather than once per client, because the bypass list is
 * a property of the target rather than of the client — and a transform can take its URL from an
 * input field, so one client serves a target host that changes from row to row. The alternative,
 * {@code RequestConfig.setProxy()}, is consulted before any route planner and returns the proxy
 * unconditionally, with nowhere to express a bypass.
 */
public class ProxyRoutePlanner extends DefaultRoutePlanner {

  /** Separators accepted in the bypass list: the JDK uses {@code |}, we also allow , and ;. */
  private static final Pattern SEPARATOR = Pattern.compile("[|,;]");

  private final HttpHost proxy;
  private final List<Pattern> bypassPatterns;

  /**
   * @param proxy the proxy to route through, or {@code null} for a direct route
   * @param nonProxyHosts hosts that bypass the proxy, in JDK {@code http.nonProxyHosts} syntax:
   *     entries separated by {@code |} (commas and semicolons work too), each optionally using
   *     {@code *} as a wildcard, for example {@code localhost|127.*|*.internal.example.com}
   */
  public ProxyRoutePlanner(HttpHost proxy, String nonProxyHosts) {
    super(null);
    this.proxy = proxy;
    this.bypassPatterns = compileBypassPatterns(nonProxyHosts);
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
