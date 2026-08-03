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

package org.apache.hop.marketplace.config;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.apache.hop.core.exception.HopException;

/**
 * Single HTTP path for every marketplace request: catalog fetches, repository browsing, definition
 * imports and artifact downloads. Keeping them together means credentials are applied — and
 * recovered from — identically everywhere.
 */
public final class MarketplaceHttp {

  private MarketplaceHttp() {}

  public static HttpClient newClient() {
    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
  }

  /**
   * GET with the repository's credentials, retrying once anonymously when the server rejects
   * credentials that came only from the environment.
   *
   * <p>{@code HOP_MARKETPLACE_USERNAME} / {@code HOP_MARKETPLACE_PASSWORD} are global, so they are
   * offered to every repository including public ones. Maven Central and the ASF repository reject
   * unknown credentials with 401 rather than ignoring them, which otherwise makes a working
   * anonymous repository look broken as soon as any private repository is configured. Credentials
   * set explicitly on the repository are never dropped this way — those are deliberate.
   */
  public static <T> HttpResponse<T> send(
      HttpClient client,
      String url,
      Duration timeout,
      MarketplaceRepository repository,
      HttpResponse.BodyHandler<T> handler)
      throws IOException, InterruptedException {

    HttpResponse<T> response = send(client, url, timeout, repository, handler, true);
    if (isAuthFailure(response.statusCode())
        && repository != null
        && repository.credentialsFromEnvironmentOnly()) {
      close(response.body());
      response = send(client, url, timeout, repository, handler, false);
    }
    return response;
  }

  private static <T> HttpResponse<T> send(
      HttpClient client,
      String url,
      Duration timeout,
      MarketplaceRepository repository,
      HttpResponse.BodyHandler<T> handler,
      boolean withCredentials)
      throws IOException, InterruptedException {
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET();
    if (withCredentials) {
      applyAuth(builder, repository);
    }
    return client.send(builder.build(), handler);
  }

  /** Authenticated GET returning the body as text. */
  public static String getText(
      HttpClient client, String url, MarketplaceRepository repository, String label)
      throws HopException {
    try {
      HttpResponse<String> response =
          send(
              client,
              url,
              Duration.ofSeconds(60),
              repository,
              HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new HopException(
            label
                + " failed HTTP "
                + response.statusCode()
                + " for "
                + url
                + authHint(response.statusCode(), repository));
      }
      return response.body();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new HopException(label + " failed for " + url, e);
    }
  }

  /**
   * HTTP Basic when credentials are configured. Forgejo accepts Basic on both the Maven endpoint
   * and the JSON API, with the access token supplied as the password.
   */
  public static void applyAuth(HttpRequest.Builder builder, MarketplaceRepository repository) {
    if (repository == null || !repository.hasCredentials()) {
      return;
    }
    String credentials = repository.effectiveUsername() + ":" + repository.effectivePassword();
    builder.header(
        "Authorization",
        "Basic "
            + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
  }

  public static boolean isAuthFailure(int status) {
    return status == 401 || status == 403;
  }

  /** Explains an auth failure in terms of what was actually sent. */
  public static String authHint(int status, MarketplaceRepository repository) {
    if (!isAuthFailure(status)) {
      return "";
    }
    if (repository == null || !repository.hasCredentials()) {
      return ". No credentials were sent; set HOP_MARKETPLACE_USERNAME / HOP_MARKETPLACE_PASSWORD"
          + " if this repository is private.";
    }
    if (repository.credentialsFromEnvironmentOnly()) {
      return ". Environment credentials for user '"
          + repository.effectiveUsername()
          + "' were rejected, and an anonymous retry also failed. Set repository-specific"
          + " credentials with HOP_MARKETPLACE_"
          + repository.environmentIdPrefix()
          + "_USERNAME / _PASSWORD if this repository needs different ones.";
    }
    return ". Basic auth was sent as user '"
        + repository.effectiveUsername()
        + "' from the repository configuration; check that password.";
  }

  private static void close(Object body) {
    if (body instanceof Closeable closeable) {
      try {
        closeable.close();
      } catch (IOException ignored) {
        // best effort: the response is being discarded anyway
      }
    }
  }

  public static String enc(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  public static String str(Object value) {
    if (value == null) {
      return null;
    }
    String s = String.valueOf(value).trim();
    return s.isEmpty() ? null : s;
  }
}
