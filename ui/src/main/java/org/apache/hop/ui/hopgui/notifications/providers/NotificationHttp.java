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
package org.apache.hop.ui.hopgui.notifications.providers;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.variables.Variables;

/** Shared HTTP setup for the notification providers. */
final class NotificationHttp {

  /** Give up if the remote host has not accepted the connection by then. */
  static final int CONNECT_TIMEOUT_MS = 10000;

  /** Give up if the remote host has not answered by then. */
  static final int RESPONSE_TIMEOUT_MS = 20000;

  private NotificationHttp() {
    // Utility class
  }

  /**
   * A client with timeouts. {@link HttpClientManager#createDefaultClient()} sets none at all, and
   * HttpClient 5 waits indefinitely for a response, so a single unresponsive feed would otherwise
   * hold a polling thread forever.
   *
   * <p>The client is built on the process-wide shared connection manager, so do not close it:
   * closing the response is what returns this request's connection to the pool.
   *
   * @return A client configured for polling a notification source
   */
  static CloseableHttpClient newClient() {
    return newClient(null, null);
  }

  /**
   * A client with timeouts, authenticating when credentials are given.
   *
   * @param username The user name, may be null or empty for anonymous access
   * @param password The password or token, may be null or empty for anonymous access
   * @return A client configured for polling a notification source
   */
  static CloseableHttpClient newClient(String username, String password) {
    HttpClientManager.HttpClientBuilderFacade builder =
        HttpClientManager.getInstance()
            .createBuilder()
            .setConnectionTimeout(CONNECT_TIMEOUT_MS)
            .setSocketTimeout(RESPONSE_TIMEOUT_MS);
    String resolvedPassword = resolve(password);
    if (resolvedPassword != null && !resolvedPassword.isEmpty()) {
      // A token is often all a source wants; GitHub, for one, ignores the user name entirely.
      builder.setCredentials(resolve(username), resolvedPassword);
    }
    return builder.build();
  }

  /**
   * Remembers what a source last answered, so the next poll can ask only for what changed.
   *
   * <p>Polling every hour otherwise re-downloads the same feed forever. A conditional request costs
   * the source almost nothing to answer with 304, and on GitHub a 304 does not count against the
   * rate limit at all.
   */
  static final class Conditional {
    private String etag;
    private String lastModified;

    /**
     * Add the validators from the previous answer, if there was one.
     *
     * @param request The request to add them to
     */
    void applyTo(org.apache.hc.core5.http.HttpRequest request) {
      if (etag != null) {
        request.addHeader("If-None-Match", etag);
      }
      if (lastModified != null) {
        request.addHeader("If-Modified-Since", lastModified);
      }
    }

    /**
     * Remember the validators of an answer.
     *
     * @param response The answer to remember
     */
    void remember(org.apache.hc.core5.http.HttpResponse response) {
      org.apache.hc.core5.http.Header tag = response.getFirstHeader("ETag");
      org.apache.hc.core5.http.Header modified = response.getFirstHeader("Last-Modified");
      if (tag != null) {
        etag = tag.getValue();
      }
      if (modified != null) {
        lastModified = modified.getValue();
      }
    }
  }

  /**
   * Resolve a credential that was stored as a variable or a variable resolver expression.
   *
   * <p>Storing the reference rather than the value keeps tokens out of the configuration file. It
   * is resolved here, at the point of use, so a rotated secret takes effect without editing the
   * source.
   *
   * @param value The configured value, may be null
   * @return The resolved value, or the value itself when there is nothing to resolve
   */
  static String resolve(String value) {
    if (value == null || value.isEmpty() || !value.contains("{")) {
      return value;
    }
    try {
      return Variables.getADefaultVariableSpace().resolve(value);
    } catch (Exception e) {
      // Resolvers reach out to metadata and secret managers. A broken one must not stop the fetch:
      // sending the expression unresolved fails visibly, with an authentication error.
      return value;
    }
  }
}
