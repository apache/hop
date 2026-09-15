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
import org.apache.commons.lang3.StringUtils;
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
    return send(client, url, null, null, timeout, repository, handler);
  }

  private static <T> HttpResponse<T> send(
      HttpClient client,
      String url,
      String body,
      String contentType,
      Duration timeout,
      MarketplaceRepository repository,
      HttpResponse.BodyHandler<T> handler)
      throws IOException, InterruptedException {

    HttpResponse<T> response =
        send(client, url, body, contentType, timeout, repository, handler, true);
    if (isAuthFailure(response.statusCode())
        && repository != null
        && repository.credentialsFromEnvironmentOnly()) {
      close(response.body());
      response = send(client, url, body, contentType, timeout, repository, handler, false);
    }
    return response;
  }

  private static <T> HttpResponse<T> send(
      HttpClient client,
      String url,
      String body,
      String contentType,
      Duration timeout,
      MarketplaceRepository repository,
      HttpResponse.BodyHandler<T> handler,
      boolean withCredentials)
      throws IOException, InterruptedException {
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url)).timeout(timeout);
    if (body == null) {
      builder.GET();
    } else {
      builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
      if (contentType != null) {
        builder.header("Content-Type", contentType);
      }
    }
    if (withCredentials) {
      applyAuth(builder, repository);
    }
    return client.send(builder.build(), handler);
  }

  /** Authenticated GET returning the body as text. */
  public static String getText(
      HttpClient client, String url, MarketplaceRepository repository, String label)
      throws HopException {
    return text(client, url, null, null, repository, label);
  }

  /**
   * Authenticated POST returning the body as text. JFrog's AQL search is the one marketplace call
   * that is a POST; its credential handling and anonymous retry have to be identical to the GET
   * path, which is why it lives here rather than in the browser.
   */
  public static String postText(
      HttpClient client,
      String url,
      String body,
      String contentType,
      MarketplaceRepository repository,
      String label)
      throws HopException {
    return text(client, url, body, contentType, repository, label);
  }

  private static String text(
      HttpClient client,
      String url,
      String body,
      String contentType,
      MarketplaceRepository repository,
      String label)
      throws HopException {
    try {
      HttpResponse<String> response =
          send(
              client,
              url,
              body,
              contentType,
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
   * Apply the repository's {@link MarketplaceRepository#effectiveAuthType() authentication type}.
   * Nothing is sent when the type is {@code none}, when it is unrecognised, or when the credentials
   * it needs do not resolve — {@link #authHint} explains which of those happened if the server then
   * answers 401.
   *
   * <p>Basic suits Nexus and Forgejo, which both accept an access token as the password. Bearer
   * suits JFrog Artifactory access tokens, which carry no username.
   */
  public static void applyAuth(HttpRequest.Builder builder, MarketplaceRepository repository) {
    if (repository == null || !repository.hasCredentials()) {
      return;
    }
    if (MarketplaceRepository.AUTH_TOKEN.equals(repository.effectiveAuthType())) {
      builder.header("Authorization", "Bearer " + repository.effectiveToken());
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
    if (repository == null) {
      return ". No credentials were sent. Enable anonymous read on the repository, or set"
          + " HOP_MARKETPLACE_USERNAME / HOP_MARKETPLACE_PASSWORD if it is private.";
    }
    if (!repository.hasCredentials()) {
      return nothingSentHint(repository);
    }
    boolean token = MarketplaceRepository.AUTH_TOKEN.equals(repository.effectiveAuthType());
    if (repository.credentialsFromEnvironmentOnly()) {
      if (token) {
        return ". An environment token was rejected, and an anonymous retry also failed. Set"
            + " HOP_MARKETPLACE_"
            + repository.environmentIdPrefix()
            + "_TOKEN to a token with read access to this repository.";
      }
      return ". Environment credentials for user '"
          + repository.effectiveUsername()
          + "' were rejected, and an anonymous retry also failed. Set repository-specific"
          + " credentials with HOP_MARKETPLACE_"
          + repository.environmentIdPrefix()
          + "_USERNAME / _PASSWORD if this repository needs different ones.";
    }
    // Token auth ignores the username, so an unresolved one there is somebody else's problem.
    if (token
        ? unresolved(repository.effectiveToken())
        : unresolved(repository.effectiveUsername())
            || unresolved(repository.effectivePassword())) {
      return ". The credentials configured for this repository use a variable that is not set, so"
          + " the expression itself was sent as the credential. Define the variable, or set"
          + " HOP_MARKETPLACE_"
          + repository.environmentIdPrefix()
          + (token ? "_TOKEN" : "_USERNAME / _PASSWORD")
          + " instead.";
    }
    if (token) {
      return ". A bearer token from the repository configuration was rejected; check the token and"
          + " that it grants read access to this repository.";
    }
    return ". Basic auth was sent as user '"
        + repository.effectiveUsername()
        + "' from the repository configuration; check that password.";
  }

  /**
   * Why no Authorization header went out. Anonymous is a valid configuration, so the useful part is
   * distinguishing "nothing is configured" from "an authType was chosen but cannot be satisfied" —
   * the second looks identical from the outside and is otherwise diagnosed by guesswork.
   */
  private static String nothingSentHint(MarketplaceRepository repository) {
    String prefix = repository.environmentIdPrefix();
    switch (repository.effectiveAuthType()) {
      case MarketplaceRepository.AUTH_NONE:
        if (MarketplaceRepository.AUTH_NONE.equalsIgnoreCase(
            StringUtils.trimToEmpty(repository.getAuthType()))) {
          return ". No credentials were sent because authType is 'none' for this repository. Remove"
              + " it, or set it to basic or token, to authenticate.";
        }
        return ". No credentials were sent. Enable anonymous read on the repository, or set"
            + " HOP_MARKETPLACE_USERNAME / HOP_MARKETPLACE_PASSWORD if it is private.";
      case MarketplaceRepository.AUTH_BASIC:
        return ". No credentials were sent: authType is 'basic' but no "
            + (StringUtils.isBlank(repository.effectiveUsername()) ? "username" : "password")
            + " resolves for this repository. Set HOP_MARKETPLACE_"
            + prefix
            + "_USERNAME / _PASSWORD, or configure them on the repository.";
      case MarketplaceRepository.AUTH_TOKEN:
        return ". No credentials were sent: authType is 'token' but no token resolves for this"
            + " repository. Set HOP_MARKETPLACE_"
            + prefix
            + "_TOKEN, or put the token in the repository password field.";
      default:
        return ". No credentials were sent: authType '"
            + repository.getAuthType()
            + "' is not recognised. Use auto, none, basic or token.";
    }
  }

  /** A credential that still carries variable syntax never made it through resolution. */
  private static boolean unresolved(String credential) {
    return credential != null && (credential.contains("${") || credential.contains("#{"));
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
