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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

/**
 * Fetches and caches OAuth 2 access tokens (issue #6595).
 *
 * <p>The cache is static and shared, which is the point rather than an optimisation. A transform
 * builds one authenticator per copy, so eight copies would otherwise mean eight token requests per
 * run and eight more whenever one expires. Token endpoints are commonly rate-limited, and some
 * providers invalidate the previous token every time they issue one — which would make the copies
 * knock each other offline. Entries are keyed on what identifies the token, so two connections
 * pointing at the same endpoint with the same client and scope share one.
 *
 * <p>The token request is issued with a client built from the caller's own {@link
 * RestClientSettings}, so it inherits the proxy, the TLS configuration and the timeouts. Fetching
 * it with a default client would work on a developer laptop and fail behind a corporate proxy,
 * while the API call it is meant to authorise succeeded.
 */
public final class RestOAuth2TokenProvider {

  /** Refresh this long before the server's own expiry, to absorb clock skew and flight time. */
  private static final long EXPIRY_SAFETY_MARGIN_MS = 30_000L;

  /** Used when the server does not say how long the token lasts. */
  private static final long DEFAULT_LIFETIME_MS = 300_000L;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<String, CachedToken> CACHE = new ConcurrentHashMap<>();

  private RestOAuth2TokenProvider() {
    // Utility class
  }

  /** A token and the moment it stops being usable. */
  private record CachedToken(String accessToken, long usableUntilMs) {
    boolean isUsable(long now) {
      return now < usableUntilMs;
    }
  }

  /**
   * The access token for these settings, fetched if there is no usable one cached.
   *
   * @param nowMs current time, passed in so the expiry logic can be tested without waiting
   */
  public static String getAccessToken(RestClientSettings settings, long nowMs) throws HopException {
    String key = cacheKey(settings);
    CachedToken cached = CACHE.get(key);
    if (cached != null && cached.isUsable(nowMs)) {
      return cached.accessToken();
    }
    // Locked per key rather than globally: a slow token endpoint for one API must not hold up
    // every other API in the same pipeline.
    synchronized (lockFor(key)) {
      cached = CACHE.get(key);
      if (cached != null && cached.isUsable(nowMs)) {
        return cached.accessToken();
      }
      CachedToken fetched = requestToken(settings, nowMs);
      CACHE.put(key, fetched);
      return fetched.accessToken();
    }
  }

  public static String getAccessToken(RestClientSettings settings) throws HopException {
    return getAccessToken(settings, System.currentTimeMillis());
  }

  /**
   * Drops the cached token for these settings, so the next request fetches a new one. Called when a
   * server rejects a token that had not expired as far as Hop knew — revoked, or rotated by another
   * client.
   */
  public static void invalidate(RestClientSettings settings) {
    CACHE.remove(cacheKey(settings));
  }

  /** Visible for tests: forget everything, so one test cannot see another's token. */
  public static void clearCache() {
    CACHE.clear();
    LOCKS.clear();
  }

  private static final Map<String, Object> LOCKS = new ConcurrentHashMap<>();

  private static Object lockFor(String key) {
    return LOCKS.computeIfAbsent(key, k -> new Object());
  }

  /**
   * What makes two requests share a token: the endpoint, the client, the grant and the scope. The
   * secret is deliberately not part of it — it is not an identity, and keeping it out means the key
   * can be logged when diagnosing a cache problem.
   */
  private static String cacheKey(RestClientSettings settings) {
    return String.join(
        "\n",
        Utils.isEmpty(settings.getOauth2TokenUrl()) ? "" : settings.getOauth2TokenUrl(),
        Utils.isEmpty(settings.getOauth2ClientId()) ? "" : settings.getOauth2ClientId(),
        settings.getOauth2Grant() == null ? "" : settings.getOauth2Grant().name(),
        Utils.isEmpty(settings.getOauth2Scope()) ? "" : settings.getOauth2Scope());
  }

  private static CachedToken requestToken(RestClientSettings settings, long nowMs)
      throws HopException {
    if (Utils.isEmpty(settings.getOauth2TokenUrl())) {
      throw new HopException("An OAuth 2 token URL is required but was not configured");
    }

    HttpPost post = new HttpPost(URI.create(settings.getOauth2TokenUrl()));
    post.setHeader(HttpHeaders.ACCEPT, "application/json");

    List<NameValuePair> form = new ArrayList<>();
    RestOAuth2Grant grant =
        settings.getOauth2Grant() == null
            ? RestOAuth2Grant.CLIENT_CREDENTIALS
            : settings.getOauth2Grant();
    form.add(new BasicNameValuePair("grant_type", grant.getWireName()));
    if (grant == RestOAuth2Grant.REFRESH_TOKEN) {
      if (Utils.isEmpty(settings.getOauth2RefreshToken())) {
        throw new HopException("A refresh token is required for the refresh_token grant");
      }
      form.add(new BasicNameValuePair("refresh_token", settings.getOauth2RefreshToken()));
    }
    if (!Utils.isEmpty(settings.getOauth2Scope())) {
      form.add(new BasicNameValuePair("scope", settings.getOauth2Scope()));
    }

    applyClientCredentials(settings, post, form);
    post.setEntity(new UrlEncodedFormEntity(form, StandardCharsets.UTF_8));

    return postForToken(settings, post, nowMs);
  }

  /**
   * RFC 6749 §2.3.1 says a server MUST support client credentials in the Authorization header and
   * MAY support them in the body. Header is therefore the safer default, with the body form
   * available for the servers that only accept that.
   */
  private static void applyClientCredentials(
      RestClientSettings settings, HttpPost post, List<NameValuePair> form) {
    String clientId =
        Utils.isEmpty(settings.getOauth2ClientId()) ? "" : settings.getOauth2ClientId();
    String clientSecret =
        Utils.isEmpty(settings.getOauth2ClientSecret()) ? "" : settings.getOauth2ClientSecret();

    // client_id always goes in the body, whichever way the secret travels. RFC 6749 §4.1.3 lists it
    // as a body parameter, and Microsoft Entra ID rejects a request without it even when a Basic
    // header carries the same value — "AADSTS900144: The request body must contain the following
    // parameter: 'client_id'". It is an identifier rather than a credential, so repeating it
    // alongside the header is not a second authentication method.
    if (!clientId.isEmpty()) {
      form.add(new BasicNameValuePair("client_id", clientId));
    }

    if (settings.isOauth2CredentialsInBody()) {
      form.add(new BasicNameValuePair("client_secret", clientSecret));
    } else if (!clientId.isEmpty()) {
      String pair = clientId + ":" + clientSecret;
      post.setHeader(
          HttpHeaders.AUTHORIZATION,
          "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8)));
    }
  }

  /**
   * Sends a prepared token request and reads the response.
   *
   * <p>Same proxy, TLS and timeouts as the API call, but no authentication of its own: the token
   * request carries the client credentials itself, and applying the connection's OAuth settings
   * here would recurse.
   */
  private static CachedToken postForToken(RestClientSettings settings, HttpPost post, long nowMs)
      throws HopException {
    try (CloseableHttpClient client = RestClientFactory.createClient(transportOnlyCopy(settings))) {
      return client.execute(post, response -> parseTokenResponse(response, nowMs));
    } catch (Exception e) {
      throw new HopException(
          "Unable to obtain an OAuth 2 access token from " + settings.getOauth2TokenUrl(), e);
    }
  }

  /** What an interactive authorization returns: the tokens the connection needs to store. */
  public record AuthorizationResult(String accessToken, String refreshToken) {}

  /**
   * Exchanges an authorization code for tokens (the design-time half of issue #6595).
   *
   * <p>This is the one step that needs a human: the user consents in a browser and brings back a
   * code. What it leaves behind is a refresh token, after which pipelines run unattended on the
   * {@link RestOAuth2Grant#REFRESH_TOKEN} grant.
   *
   * <p>It goes through the same client as everything else, so a proxy configured on the connection
   * applies here too — authorizing from behind a corporate proxy is exactly when you would want it
   * to.
   */
  public static AuthorizationResult exchangeAuthorizationCode(
      RestClientSettings settings, String code, String codeVerifier) throws HopException {
    if (Utils.isEmpty(settings.getOauth2TokenUrl())) {
      throw new HopException("An OAuth 2 token URL is required to exchange the authorization code");
    }
    // Trimmed: a code is copied out of a browser address bar, so trailing whitespace is normal and
    // a value that is *only* whitespace should say so here rather than be rejected by the server.
    String trimmedCode = code == null ? "" : code.trim();
    if (trimmedCode.isEmpty()) {
      throw new HopException("No authorization code was supplied");
    }

    HttpPost post = new HttpPost(URI.create(settings.getOauth2TokenUrl()));
    post.setHeader(HttpHeaders.ACCEPT, "application/json");

    List<NameValuePair> form = new ArrayList<>();
    form.add(new BasicNameValuePair("grant_type", "authorization_code"));
    form.add(new BasicNameValuePair("code", trimmedCode));
    if (!Utils.isEmpty(settings.getOauth2RedirectUri())) {
      form.add(new BasicNameValuePair("redirect_uri", settings.getOauth2RedirectUri()));
    }
    if (!Utils.isEmpty(codeVerifier)) {
      form.add(new BasicNameValuePair("code_verifier", codeVerifier));
    }
    applyClientCredentials(settings, post, form);
    post.setEntity(new UrlEncodedFormEntity(form, StandardCharsets.UTF_8));

    try (CloseableHttpClient client = RestClientFactory.createClient(transportOnlyCopy(settings))) {
      return client.execute(post, RestOAuth2TokenProvider::parseAuthorizationResponse);
    } catch (Exception e) {
      throw new HopException(
          "Unable to exchange the authorization code at " + settings.getOauth2TokenUrl(), e);
    }
  }

  private static AuthorizationResult parseAuthorizationResponse(
      org.apache.hc.core5.http.ClassicHttpResponse response) throws IOException {
    String body = readBody(response);
    if (response.getCode() < 200 || response.getCode() >= 300) {
      throw new IOException(
          "The token endpoint answered " + response.getCode() + ": " + abbreviate(body));
    }
    JsonNode json = MAPPER.readTree(body);
    JsonNode refresh = json.get("refresh_token");
    if (refresh == null || Utils.isEmpty(refresh.asText())) {
      // Without one, every pipeline run would need a human again — which defeats the point.
      throw new IOException(
          "The authorization succeeded but returned no refresh_token. Add the scope that asks for "
              + "offline access - 'offline_access' on Microsoft Entra ID, Google and most others - "
              + "and authorize again. The granted scope is in the response below: "
              + abbreviate(body));
    }
    JsonNode access = json.get("access_token");
    return new AuthorizationResult(access == null ? null : access.asText(), refresh.asText());
  }

  private static CachedToken parseTokenResponse(
      org.apache.hc.core5.http.ClassicHttpResponse response, long nowMs) throws IOException {
    String body = readBody(response);

    if (response.getCode() < 200 || response.getCode() >= 300) {
      // The body of an OAuth error names the reason ("invalid_client", "invalid_scope"), which is
      // the only thing that makes these diagnosable — so it goes in the message.
      throw new IOException(
          "The token endpoint answered " + response.getCode() + ": " + abbreviate(body));
    }

    JsonNode json = MAPPER.readTree(body);
    JsonNode token = json.get("access_token");
    if (token == null || Utils.isEmpty(token.asText())) {
      throw new IOException("The token endpoint returned no access_token: " + abbreviate(body));
    }

    long lifetimeMs = DEFAULT_LIFETIME_MS;
    JsonNode expiresIn = json.get("expires_in");
    if (expiresIn != null && expiresIn.asLong(0) > 0) {
      lifetimeMs = expiresIn.asLong() * 1000L;
    }
    long usableFor = Math.max(0, lifetimeMs - EXPIRY_SAFETY_MARGIN_MS);
    return new CachedToken(token.asText(), nowMs + usableFor);
  }

  private static String readBody(org.apache.hc.core5.http.ClassicHttpResponse response)
      throws IOException {
    HttpEntity entity = response.getEntity();
    try {
      return entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);
    } catch (org.apache.hc.core5.http.ParseException e) {
      throw new IOException("The token endpoint returned an unreadable body", e);
    }
  }

  /** Everything about reaching the server, nothing about authenticating to it. */
  private static RestClientSettings transportOnlyCopy(RestClientSettings settings) {
    RestClientSettings copy = new RestClientSettings();
    copy.setConnectTimeout(settings.getConnectTimeout());
    copy.setReadTimeout(settings.getReadTimeout());
    copy.setProxyScheme(settings.getProxyScheme());
    copy.setProxyHost(settings.getProxyHost());
    copy.setProxyPort(settings.getProxyPort());
    copy.setProxyUsername(settings.getProxyUsername());
    copy.setProxyPassword(settings.getProxyPassword());
    copy.setNonProxyHosts(settings.getNonProxyHosts());
    copy.setSslContext(settings.getSslContext());
    copy.setPermissiveHostnameVerifier(settings.isPermissiveHostnameVerifier());
    copy.setAuthType(RestAuthType.NONE);
    return copy;
  }

  /** Token values that must never reach an error message, a log file or a ticket. */
  private static final Pattern TOKEN_VALUES =
      Pattern.compile("(\"(?:access_token|refresh_token|id_token)\"\\s*:\\s*\")([^\"]*)(\")");

  /**
   * Trims a response body down to something a person can read in an error message, with any token
   * values redacted.
   *
   * <p>The body is the only thing that makes an OAuth failure diagnosable — it carries {@code
   * invalid_client}, {@code AADSTS…} and the granted scope — so it has to be shown. But a
   * successful-looking response also carries live credentials, and these messages end up in error
   * dialogs, log files and issue reports.
   */
  private static String abbreviate(String body) {
    String trimmed = body == null ? "" : body.trim();
    String redacted = TOKEN_VALUES.matcher(trimmed).replaceAll("$1********$3");
    return redacted.length() > 500 ? redacted.substring(0, 500) + "..." : redacted;
  }
}
