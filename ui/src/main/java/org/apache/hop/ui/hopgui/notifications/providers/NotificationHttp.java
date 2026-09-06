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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.hopgui.notifications.NotificationLinks;

/** Shared HTTP setup for the notification providers. */
final class NotificationHttp {

  /** Give up if the remote host has not accepted the connection by then. */
  static final int CONNECT_TIMEOUT_MS = 10000;

  /** Give up if the remote host has not answered by then. */
  static final int RESPONSE_TIMEOUT_MS = 20000;

  /**
   * Stop reading a source that will not stop talking.
   *
   * <p>Neither provider knows how much it is about to read: a feed is streamed straight into a DOM
   * parser and the GitHub answer straight into Jackson, so a source that answers with gigabytes -
   * broken, hostile, or simply a misconfigured proxy - takes the process down with it. No real feed
   * or releases page comes close to this.
   */
  static final long MAX_RESPONSE_BYTES = 8L * 1024 * 1024;

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
    return newClient(null, null, null);
  }

  /**
   * A client with timeouts, authenticating when credentials are given.
   *
   * <p>The credentials are scoped to {@code target}. {@link
   * HttpClientManager.HttpClientBuilderFacade#setCredentials(String, String)} registers {@code
   * AuthScope(null, null, -1, null, null)}, which matches every host, so a source that redirects
   * somewhere else and answers 401 would be offered this source's token. Redirects are followed by
   * default and a feed is a remote party's URL, so that host is not ours to trust.
   *
   * @param target The URL about to be requested, which is the only origin these credentials are
   *     for; null for an anonymous client
   * @param username The user name, may be null or empty for a token-only source
   * @param password The password or token, may be null or empty for anonymous access
   * @return A client configured for polling a notification source
   */
  static CloseableHttpClient newClient(URI target, String username, String password) {
    HttpClientManager.HttpClientBuilderFacade builder =
        HttpClientManager.getInstance()
            .createBuilder()
            .setConnectionTimeout(CONNECT_TIMEOUT_MS)
            .setSocketTimeout(RESPONSE_TIMEOUT_MS);
    String resolvedPassword = resolve(password);
    if (target != null && resolvedPassword != null && !resolvedPassword.isEmpty()) {
      // A token is often all a source wants; GitHub, for one, ignores the user name entirely.
      // HttpClient 5 will not build a credential without a user name, so a source that stores only
      // a token gets an empty one rather than an IllegalArgumentException on the first poll.
      String resolvedUsername = resolve(username);
      builder.setCredentials(
          resolvedUsername == null ? "" : resolvedUsername,
          resolvedPassword,
          new AuthScope(HttpClientManager.createHttpHost(target)));
    }
    return builder.build();
  }

  /**
   * The URL a source is configured with, as a URI we are willing to request.
   *
   * <p>A stored URL is whatever the user typed, and it ends up both here and, through {@code
   * HttpGet}, at whatever the scheme handler does with it. Only absolute http and https URLs naming
   * a host are accepted, so a source cannot make Hop read {@code file:} or reach a JVM protocol
   * handler.
   *
   * @param url The configured URL
   * @return The parsed URL
   * @throws HopException When the URL is not one we will request
   */
  static URI requestable(String url) throws HopException {
    if (!NotificationLinks.isSafe(url)) {
      throw new HopException(
          "Refusing to poll '" + url + "': only absolute http and https URLs are requested.");
    }
    return URI.create(url.trim());
  }

  /**
   * Wrap a response body so that reading it cannot run away.
   *
   * @param stream The response body
   * @param source The URL it came from, for the error message
   * @return A stream that fails once {@link #MAX_RESPONSE_BYTES} have been read
   */
  static InputStream bounded(InputStream stream, String source) {
    return new BoundedStream(stream, source);
  }

  /** Fails rather than truncating: a half-read feed would only fail later, less clearly. */
  private static final class BoundedStream extends FilterInputStream {
    private final String source;
    private long read;

    private BoundedStream(InputStream in, String source) {
      super(in);
      this.source = source;
    }

    private void count(long justRead) throws IOException {
      if (justRead <= 0) {
        return;
      }
      read += justRead;
      if (read > MAX_RESPONSE_BYTES) {
        throw new IOException(
            "The source at "
                + source
                + " answered with more than "
                + (MAX_RESPONSE_BYTES / (1024 * 1024))
                + " MB, which is more than a feed or a releases page should ever be.");
      }
    }

    @Override
    public int read() throws IOException {
      int b = super.read();
      count(b == -1 ? 0 : 1);
      return b;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int justRead = super.read(buffer, offset, length);
      count(justRead);
      return justRead;
    }
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
