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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpMessage;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

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

  private static final Class<?> PKG = RestAuthenticator.class;

  private static final String BEARER_PREFIX = "Bearer ";

  /** How many applied OAuth 2 tokens to remember. A copy only ever has one or two in flight. */
  private static final int APPLIED_TOKENS_REMEMBERED = 8;

  private final RestClientSettings settings;

  /**
   * The OAuth 2 access tokens this authenticator put on a request, most recent last. A 401 is only
   * answered with a new token when the rejected one is in here: an Authorization header a row
   * supplied itself is the row's business, and swapping it for a token of ours would change who the
   * request is made as.
   */
  private final Map<String, Boolean> appliedOAuth2Tokens = rememberedTokens();

  /** Tokens a server rejected again straight after a renewal: see {@link #renewedTokenRejected}. */
  private final Map<String, Boolean> tokensRejectedAfterRenewal = rememberedTokens();

  /**
   * The origin the credentials and static headers are scoped to instead of the settings' own, or
   * {@code null} to use the settings' one.
   */
  private final String authOrigin;

  public RestAuthenticator(RestClientSettings settings) {
    this(settings, null);
  }

  /**
   * An authenticator scoped to another origin than the settings name. The connection test uses it
   * for its one request to an explicitly named URL, without copying the settings.
   *
   * @param authOrigin the URL whose scheme, host and port the credentials are sent to
   */
  public RestAuthenticator(RestClientSettings settings, String authOrigin) {
    this.settings = settings;
    this.authOrigin = authOrigin;
  }

  private static Map<String, Boolean> rememberedTokens() {
    return Collections.synchronizedMap(
        new LinkedHashMap<>() {
          @Override
          protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > APPLIED_TOKENS_REMEMBERED;
          }
        });
  }

  /**
   * Adds the authentication headers and the connection's static headers for one request. Headers
   * already on the map win: a row that supplies its own {@code Authorization} (or the configured
   * API-key header, or a header with the name of a static header) keeps it. The authentication
   * header is written before the static headers, so it also wins over a static header of the same
   * name.
   *
   * @param headers the outbound header map, modified in place
   * @param requestUri the URI this request is going to
   */
  public void applyRequestHeaders(Map<String, String> headers, String requestUri) {
    applyAuthenticationHeaders(headers, requestUri);
    applyStaticHeaders(headers, requestUri);
  }

  /**
   * Adds the authentication headers and the connection's static headers to a request object, for
   * the callers that build one themselves (HTTP client, HTTP POST, the HTTP action). Call it after
   * the request's own headers are set and before any default the caller only adds when nothing else
   * did, such as a default {@code Content-Type}.
   *
   * <p>The authentication headers replace a header of the same name, as they always have on these
   * callers. A static header is only added where the request does not already carry one with that
   * name.
   *
   * @param request the outbound request, modified in place
   * @param requestUri the URI this request is going to
   */
  public void applyTo(HttpMessage request, String requestUri) {
    if (request == null) {
      return;
    }
    Map<String, String> authHeaders = new LinkedHashMap<>();
    applyAuthenticationHeaders(authHeaders, requestUri);
    authHeaders.forEach(request::setHeader);
    if (!hasStaticHeaders() || !targetMatchesOrigin(requestUri)) {
      return;
    }
    for (Map.Entry<String, String> header : settings.getStaticHeaders().entrySet()) {
      if (!headerAlreadySupplied(request, header.getKey())) {
        request.setHeader(header.getKey(), header.getValue() == null ? "" : header.getValue());
      }
    }
  }

  private void applyAuthenticationHeaders(Map<String, String> headers, String requestUri) {
    if (headers == null || !targetMatchesOrigin(requestUri)) {
      return;
    }
    switch (settings.getAuthType()) {
      case BASIC -> applyBasic(headers);
      case BEARER -> applyBearer(headers);
      case API_KEY -> applyApiKey(headers);
      case OAUTH2 -> applyOAuth2(headers);
      case NONE -> {
        // Nothing to add.
      }
    }
  }

  /** Adds the connection's static headers (issue #9551) that the map does not already carry. */
  private void applyStaticHeaders(Map<String, String> headers, String requestUri) {
    if (headers == null || !hasStaticHeaders() || !targetMatchesOrigin(requestUri)) {
      return;
    }
    for (Map.Entry<String, String> header : settings.getStaticHeaders().entrySet()) {
      if (!headerAlreadySupplied(headers, header.getKey())) {
        // Replace an empty header of the same name rather than add a second one next to it.
        headers.keySet().removeIf(name -> name != null && name.equalsIgnoreCase(header.getKey()));
        headers.put(header.getKey(), header.getValue() == null ? "" : header.getValue());
      }
    }
  }

  private boolean hasStaticHeaders() {
    return settings.getStaticHeaders() != null && !settings.getStaticHeaders().isEmpty();
  }

  /**
   * Whether a request that came back 401 carried an OAuth 2 access token this authenticator put
   * there, and a new token is worth trying (issue #9550). An {@code Authorization} header a row or
   * the transform supplied itself is the row's business: swapping it for a token of ours would
   * change who the request is made as. A token that was rejected again straight after a renewal is
   * not renewed again either.
   *
   * @param authorization the {@code Authorization} header the rejected request was sent with
   * @param requestUri the URI the request went to
   */
  public boolean isRenewable(String authorization, String requestUri) {
    String rejected = bearerToken(authorization);
    return rejected != null
        && settings.getAuthType() == RestAuthType.OAUTH2
        && targetMatchesOrigin(requestUri)
        && appliedOAuth2Tokens.containsKey(rejected)
        && !tokensRejectedAfterRenewal.containsKey(rejected);
  }

  /**
   * Drops a token the server rejected from the shared cache (unless another request already
   * replaced it) and returns the {@code Authorization} header to send the request with once more,
   * or {@code null} when the token endpoint handed the same token back. Call it only when {@link
   * #isRenewable} is true.
   *
   * <p>Without this, a token the server stopped accepting early (revoked, rotated, or invalidated
   * because another client asked for a new one) keeps being sent until its expiry time passes.
   *
   * @param authorization the {@code Authorization} header the rejected request was sent with
   * @return the new {@code Authorization} header, or {@code null} when there is nothing new to send
   * @throws HopException when a new token cannot be obtained
   */
  public String renewRejectedToken(String authorization) throws HopException {
    String rejected = bearerToken(authorization);
    if (rejected == null) {
      return null;
    }
    RestOAuth2TokenProvider.invalidate(settings, rejected);
    String current = RestOAuth2TokenProvider.getAccessToken(settings);
    if (rejected.equals(current)) {
      // The token endpoint handed the same token back: sending it again gets the same answer.
      return null;
    }
    appliedOAuth2Tokens.put(current, Boolean.TRUE);
    return BEARER_PREFIX + current;
  }

  /**
   * Drops a rejected token from the shared cache without fetching a new one, for a request that
   * cannot be sent again (its body was streamed). The next request fetches a new token.
   *
   * @param authorization the {@code Authorization} header the rejected request was sent with
   */
  public void dropRejectedToken(String authorization) {
    String rejected = bearerToken(authorization);
    if (rejected != null) {
      RestOAuth2TokenProvider.invalidate(settings, rejected);
    }
  }

  /**
   * Called when the request sent once more with a renewed token got a 401 again. That token is not
   * renewed on later requests: a new token did not fix what the server objects to (a missing
   * permission or scope, say), and renewing on every request would only fetch token after token and
   * drop the one every pipeline in this JVM shares. It still expires and is replaced as usual.
   *
   * @param authorization the {@code Authorization} header the second request was sent with
   */
  public void renewedTokenRejected(String authorization) {
    String token = bearerToken(authorization);
    if (token != null) {
      tokensRejectedAfterRenewal.put(token, Boolean.TRUE);
    }
  }

  /** The token of a Bearer {@code Authorization} header, or {@code null}. */
  private static String bearerToken(String authorization) {
    if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
      return null;
    }
    return authorization.substring(BEARER_PREFIX.length());
  }

  /**
   * A hint for the log or an error message when a request got a 401, or {@code null} when there is
   * nothing to add. A fixed Bearer token that the server stopped accepting cannot be renewed by
   * Hop; tokens that expire need the OAuth 2 authentication type, which fetches a new one.
   */
  public String unauthorizedHint() {
    if (settings.getAuthType() == RestAuthType.BEARER) {
      return BaseMessages.getString(PKG, "RestAuthenticator.Hint.BearerUnauthorized");
    }
    return null;
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

  /**
   * Fetches (or reuses) an access token and sends it as a Bearer header. Unlike the other schemes
   * this one talks to the network, so it can fail; a token that cannot be obtained is an error
   * rather than a request sent unauthenticated, which would only produce a confusing 401 later.
   */
  private void applyOAuth2(Map<String, String> headers) {
    if (headerAlreadySupplied(headers, HttpHeaders.AUTHORIZATION)) {
      return;
    }
    try {
      String token = RestOAuth2TokenProvider.getAccessToken(settings);
      appliedOAuth2Tokens.put(token, Boolean.TRUE);
      headers.put(HttpHeaders.AUTHORIZATION, BEARER_PREFIX + token);
    } catch (HopException e) {
      throw new HopRuntimeException(e);
    }
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
    headers.put(HttpHeaders.AUTHORIZATION, BEARER_PREFIX + settings.getBearerToken());
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

  /** True when the request already carries this header with a value of its own. */
  private static boolean headerAlreadySupplied(HttpMessage request, String name) {
    for (Header header : request.getHeaders(name)) {
      if (header.getValue() != null && !Utils.isEmpty(header.getValue().trim())) {
        return true;
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
    return hostOf(authOrigin != null ? authOrigin : settings.getAuthOrigin());
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
