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

package org.apache.hop.ui.hopgui;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.client.service.JavaScriptExecutor;

/**
 * RAP implementation of {@link IHopWebUrlUpdater}. Updates the browser URL when the user switches
 * tabs and builds shareable URLs for "Copy URL for this tab".
 *
 * <p>Browser history updates use a <em>relative</em> URL (path + query) so {@code
 * history.replaceState} works when Hop Web sits behind a TLS-terminating reverse proxy (browser
 * origin is {@code https://…} while {@link HttpServletRequest#getRequestURL()} is often {@code
 * http://…}). Absolute shareable links honor {@code X-Forwarded-Proto} / {@code X-Forwarded-Host}.
 */
public class RapHopWebUrlUpdater implements IHopWebUrlUpdater {

  @Override
  public void updateUrl(String projectName, String filePath) {
    String relative = buildRelativeUrl(projectName, filePath);
    if (relative == null) {
      return;
    }
    JavaScriptExecutor executor = RWT.getClient().getService(JavaScriptExecutor.class);
    String escaped = relative.replace("\\", "\\\\").replace("'", "\\'");
    // Relative URL avoids mixed http/https; try/catch so a rare browser rejection does not kill RAP
    executor.execute(
        "try{history.replaceState(null, '', '" + escaped + "');}catch(e){/* ignore */}");
  }

  @Override
  public String buildUrl(String projectName, String filePath) {
    HttpServletRequest request = RWT.getRequest();
    if (request == null) {
      return null;
    }
    String path = requestPath(request);
    String query = buildQuery(projectName, filePath);
    return publicOrigin(request) + path + (query.isEmpty() ? "" : "?" + query);
  }

  /**
   * Path + query only (for history API). Safe across reverse proxies and mixed http/https origins.
   */
  String buildRelativeUrl(String projectName, String filePath) {
    HttpServletRequest request = RWT.getRequest();
    if (request == null) {
      return null;
    }
    String path = requestPath(request);
    String query = buildQuery(projectName, filePath);
    return path + (query.isEmpty() ? "" : "?" + query);
  }

  static String buildQuery(String projectName, String filePath) {
    StringBuilder sb = new StringBuilder();
    if (projectName != null && !projectName.isEmpty()) {
      sb.append("project=").append(encode(projectName));
    }
    if (filePath != null && !filePath.isEmpty()) {
      if (!sb.isEmpty()) {
        sb.append('&');
      }
      sb.append("file=").append(encode(filePath));
    }
    return sb.toString();
  }

  /** Request path without query or jsessionid matrix parameter. */
  static String requestPath(HttpServletRequest request) {
    String uri = request.getRequestURI();
    if (uri == null || uri.isEmpty()) {
      return "/";
    }
    int semi = uri.indexOf(';');
    if (semi >= 0) {
      uri = uri.substring(0, semi);
    }
    return uri.isEmpty() ? "/" : uri;
  }

  /**
   * Public origin as seen by the browser. Prefers {@code X-Forwarded-Proto} / {@code
   * X-Forwarded-Host} (and optional {@code X-Forwarded-Port}) when present.
   */
  static String publicOrigin(HttpServletRequest request) {
    String scheme = firstForwardedValue(request.getHeader("X-Forwarded-Proto"));
    if (scheme == null || scheme.isBlank()) {
      scheme = request.getScheme();
    }
    if (scheme == null || scheme.isBlank()) {
      scheme = "http";
    }
    scheme = scheme.toLowerCase();

    String hostHeader = firstForwardedValue(request.getHeader("X-Forwarded-Host"));
    String host;
    int port = -1;
    if (hostHeader != null && !hostHeader.isBlank()) {
      // host[:port]
      int colon = hostHeader.lastIndexOf(':');
      // IPv6 literals are [addr]:port — only split when not a bare IPv6
      if (colon > 0 && !hostHeader.startsWith("[")) {
        host = hostHeader.substring(0, colon);
        try {
          port = Integer.parseInt(hostHeader.substring(colon + 1).trim());
        } catch (NumberFormatException e) {
          host = hostHeader;
          port = -1;
        }
      } else if (hostHeader.startsWith("[") && hostHeader.contains("]:")) {
        int end = hostHeader.indexOf("]:");
        host = hostHeader.substring(0, end + 1);
        try {
          port = Integer.parseInt(hostHeader.substring(end + 2).trim());
        } catch (NumberFormatException e) {
          host = hostHeader;
          port = -1;
        }
      } else {
        host = hostHeader;
      }
    } else {
      host = request.getServerName();
      port = request.getServerPort();
    }

    String forwardedPort = firstForwardedValue(request.getHeader("X-Forwarded-Port"));
    if (forwardedPort != null && !forwardedPort.isBlank() && port < 0) {
      try {
        port = Integer.parseInt(forwardedPort.trim());
      } catch (NumberFormatException ignored) {
        // keep resolved port
      }
    }

    StringBuilder origin = new StringBuilder();
    origin.append(scheme).append("://").append(host != null ? host : "localhost");
    boolean defaultPort =
        ("http".equals(scheme) && port == 80)
            || ("https".equals(scheme) && port == 443)
            || port <= 0;
    if (!defaultPort) {
      origin.append(':').append(port);
    }
    return origin.toString();
  }

  /** First value of a possibly comma-separated forwarded header. */
  static String firstForwardedValue(String header) {
    if (header == null || header.isBlank()) {
      return null;
    }
    int comma = header.indexOf(',');
    String value = comma >= 0 ? header.substring(0, comma) : header;
    return value.trim();
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Override
  public void copyToClipboard(String text) {
    if (text == null) {
      return;
    }
    // Escape for use inside a JavaScript string (single-quoted)
    String escaped =
        text.replace("\\", "\\\\").replace("'", "\\'").replace("\r", "\\r").replace("\n", "\\n");
    JavaScriptExecutor executor = RWT.getClient().getService(JavaScriptExecutor.class);
    String script =
        "(function(){ var t = '"
            + escaped
            + "';"
            + "if (navigator.clipboard && navigator.clipboard.writeText) {"
            + "navigator.clipboard.writeText(t).catch(function(){});"
            + "} else {"
            + "var ta = document.createElement('textarea'); ta.value = t;"
            + "ta.style.position = 'fixed'; ta.style.left = '-9999px';"
            + "document.body.appendChild(ta); ta.select();"
            + "try { document.execCommand('copy'); } catch(e) {}"
            + "document.body.removeChild(ta);"
            + "}})();";
    executor.execute(script);
  }
}
