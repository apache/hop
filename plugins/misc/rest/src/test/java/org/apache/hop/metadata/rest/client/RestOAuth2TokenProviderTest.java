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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #6595: OAuth 2 against a stub token endpoint in this JVM.
 *
 * <p>The assertions that carry weight here are the caching ones. A transform builds one
 * authenticator per copy, so without a shared cache a pipeline would hammer the token endpoint —
 * and providers that rotate the token on every issue would have the copies invalidating each
 * other's credentials.
 */
class RestOAuth2TokenProviderTest {

  private HttpServer tokenServer;
  private final AtomicInteger issued = new AtomicInteger();
  private final List<String> requestBodies = new CopyOnWriteArrayList<>();
  private final List<String> authorizationHeaders = new CopyOnWriteArrayList<>();

  /** Seconds reported as expires_in; 0 means "omit it". */
  private volatile int expiresIn = 3600;

  private volatile int statusCode = 200;
  private volatile String errorBody = "";

  /** When set, the token response carries this refresh_token as well. */
  private volatile String refreshTokenInResponse;

  @BeforeEach
  void startTokenServer() throws IOException {
    RestOAuth2TokenProvider.clearCache();
    issued.set(0);
    requestBodies.clear();
    authorizationHeaders.clear();
    expiresIn = 3600;
    statusCode = 200;
    errorBody = "";
    refreshTokenInResponse = null;

    tokenServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    tokenServer.createContext("/token", this::issueToken);
    tokenServer.start();
  }

  @AfterEach
  void stopTokenServer() {
    tokenServer.stop(0);
    RestOAuth2TokenProvider.clearCache();
  }

  @Test
  void aTokenIsFetchedAndSentAsABearerHeader() throws Exception {
    RestClientSettings settings = clientCredentials();

    Map<String, String> headers = new LinkedHashMap<>();
    new RestAuthenticator(settings).applyRequestHeaders(headers, "https://api.example.com/things");

    assertEquals("Bearer token-1", headers.get("Authorization"));
    assertEquals(1, issued.get());
  }

  @Test
  void theClientCredentialsGrantSendsWhatTheSpecSays() throws Exception {
    RestOAuth2TokenProvider.getAccessToken(clientCredentials());

    String body = requestBodies.get(0);
    assertTrue(body.contains("grant_type=client_credentials"), body);
    assertTrue(body.contains("scope=read+things") || body.contains("scope=read%20things"), body);
    // RFC 6749 §2.3.1: the header form is the one every server must accept, so it is the default.
    assertEquals("id:secret", decodeBasic(authorizationHeaders.get(0)));
    assertTrue(!body.contains("client_secret"), "the secret must not also be in the body: " + body);
  }

  @Test
  void clientIdIsAlwaysInTheBodyEvenWhenTheSecretIsInTheHeader() throws Exception {
    // Microsoft Entra ID answers "AADSTS900144: The request body must contain the following
    // parameter: 'client_id'" when only the Basic header carries it. RFC 6749 §4.1.3 lists
    // client_id as a body parameter, so sending it in both places is correct and compatible.
    RestOAuth2TokenProvider.getAccessToken(clientCredentials());

    assertTrue(requestBodies.get(0).contains("client_id=id"), requestBodies.get(0));
    assertEquals("id:secret", decodeBasic(authorizationHeaders.get(0)));
  }

  @Test
  void theAuthorizationCodeExchangeAlsoCarriesClientIdInTheBody() throws Exception {
    RestClientSettings settings = clientCredentials();
    refreshTokenInResponse = "refresh-me";

    RestOAuth2TokenProvider.exchangeAuthorizationCode(settings, "the-code", "v");

    // This is the request that actually failed against Entra.
    assertTrue(requestBodies.get(0).contains("client_id=id"), requestBodies.get(0));
  }

  @Test
  void theRefreshGrantCarriesClientIdInTheBodyToo() throws Exception {
    RestClientSettings settings = clientCredentials();
    settings.setOauth2Grant(RestOAuth2Grant.REFRESH_TOKEN);
    settings.setOauth2RefreshToken("refresh-me");

    RestOAuth2TokenProvider.getAccessToken(settings);

    assertTrue(requestBodies.get(0).contains("client_id=id"), requestBodies.get(0));
  }

  @Test
  void credentialsCanGoInTheBodyForServersThatDemandIt() throws Exception {
    RestClientSettings settings = clientCredentials();
    settings.setOauth2CredentialsInBody(true);

    RestOAuth2TokenProvider.getAccessToken(settings);

    String body = requestBodies.get(0);
    assertTrue(body.contains("client_id=id"), body);
    assertTrue(body.contains("client_secret=secret"), body);
    assertTrue(authorizationHeaders.get(0) == null, "no Authorization header in this mode");
  }

  @Test
  void theRefreshTokenGrantSendsTheRefreshToken() throws Exception {
    RestClientSettings settings = clientCredentials();
    settings.setOauth2Grant(RestOAuth2Grant.REFRESH_TOKEN);
    settings.setOauth2RefreshToken("refresh-me");

    RestOAuth2TokenProvider.getAccessToken(settings);

    String body = requestBodies.get(0);
    assertTrue(body.contains("grant_type=refresh_token"), body);
    assertTrue(body.contains("refresh_token=refresh-me"), body);
  }

  @Test
  void aRefreshGrantWithoutATokenFailsBeforeReachingTheNetwork() {
    RestClientSettings settings = clientCredentials();
    settings.setOauth2Grant(RestOAuth2Grant.REFRESH_TOKEN);

    HopException e =
        assertThrows(HopException.class, () -> RestOAuth2TokenProvider.getAccessToken(settings));

    assertTrue(e.getMessage().contains("refresh token"), e.getMessage());
    assertEquals(0, issued.get());
  }

  @Test
  void manyAuthenticatorsShareOneToken() throws Exception {
    // Stands in for the copies of one transform: each builds its own settings and authenticator.
    for (int copy = 0; copy < 8; copy++) {
      Map<String, String> headers = new LinkedHashMap<>();
      new RestAuthenticator(clientCredentials())
          .applyRequestHeaders(headers, "https://api.example.com/things");
      assertEquals("Bearer token-1", headers.get("Authorization"));
    }
    assertEquals(1, issued.get(), "eight copies must not mean eight token requests");
  }

  @Test
  void aDifferentScopeGetsItsOwnToken() throws Exception {
    RestOAuth2TokenProvider.getAccessToken(clientCredentials());

    RestClientSettings other = clientCredentials();
    other.setOauth2Scope("write things");
    RestOAuth2TokenProvider.getAccessToken(other);

    assertEquals(2, issued.get(), "scope is part of what identifies a token");
  }

  @Test
  void anExpiredTokenIsRefetched() throws Exception {
    RestClientSettings settings = clientCredentials();
    expiresIn = 60;

    assertEquals("token-1", RestOAuth2TokenProvider.getAccessToken(settings, 0L));
    // Still inside the usable window (60s less the 30s safety margin).
    assertEquals("token-1", RestOAuth2TokenProvider.getAccessToken(settings, 20_000L));
    // Past it: the margin means Hop refreshes before the server would actually reject the token.
    assertEquals("token-2", RestOAuth2TokenProvider.getAccessToken(settings, 40_000L));
    assertEquals(2, issued.get());
  }

  @Test
  void aTokenWithNoExpiryStillGetsRefreshedEventually() throws Exception {
    RestClientSettings settings = clientCredentials();
    expiresIn = 0; // the server says nothing

    assertEquals("token-1", RestOAuth2TokenProvider.getAccessToken(settings, 0L));
    assertEquals("token-1", RestOAuth2TokenProvider.getAccessToken(settings, 60_000L));
    assertEquals("token-2", RestOAuth2TokenProvider.getAccessToken(settings, 600_000L));
  }

  @Test
  void invalidateForcesAFreshToken() throws Exception {
    RestClientSettings settings = clientCredentials();
    assertEquals("token-1", RestOAuth2TokenProvider.getAccessToken(settings));

    RestOAuth2TokenProvider.invalidate(settings);

    assertEquals("token-2", RestOAuth2TokenProvider.getAccessToken(settings));
  }

  @Test
  void theReasonAnEndpointRefusedIsInTheMessage() {
    statusCode = 401;
    errorBody = "{\"error\":\"invalid_client\"}";

    HopException e =
        assertThrows(
            HopException.class, () -> RestOAuth2TokenProvider.getAccessToken(clientCredentials()));

    // Without the body these are undiagnosable: every misconfiguration looks like the same 401.
    assertTrue(
        e.getMessage().contains("invalid_client") || causeContains(e, "invalid_client"),
        e.toString());
  }

  @Test
  void aResponseWithoutAnAccessTokenIsAnError() {
    errorBody = "{\"not_a_token\":true}";
    statusCode = 200;

    HopException e =
        assertThrows(
            HopException.class, () -> RestOAuth2TokenProvider.getAccessToken(clientCredentials()));

    assertTrue(
        e.getMessage().contains("access_token") || causeContains(e, "access_token"), e.toString());
  }

  @Test
  void anAuthorizationCodeIsExchangedForARefreshToken() throws Exception {
    RestClientSettings settings = clientCredentials();
    settings.setOauth2RedirectUri("http://localhost:8080/callback");
    refreshTokenInResponse = "refresh-me";

    RestOAuth2TokenProvider.AuthorizationResult result =
        RestOAuth2TokenProvider.exchangeAuthorizationCode(settings, "the-code", "the-verifier");

    assertEquals("refresh-me", result.refreshToken());
    String body = requestBodies.get(0);
    assertTrue(body.contains("grant_type=authorization_code"), body);
    assertTrue(body.contains("code=the-code"), body);
    // PKCE: without the verifier an intercepted code would be redeemable by anyone.
    assertTrue(body.contains("code_verifier=the-verifier"), body);
    assertTrue(body.contains("redirect_uri=http%3A%2F%2Flocalhost%3A8080%2Fcallback"), body);
  }

  @Test
  void anAuthorizationThatReturnsNoRefreshTokenSaysWhatToDoAboutIt() {
    RestClientSettings settings = clientCredentials();
    refreshTokenInResponse = null; // the server issued only an access token

    HopException e =
        assertThrows(
            HopException.class,
            () -> RestOAuth2TokenProvider.exchangeAuthorizationCode(settings, "the-code", "v"));

    // Silently succeeding here would leave a connection that works until the access token expires
    // and then needs a human again, which is the thing this flow exists to avoid.
    assertTrue(causeContains(e, "offline access"), e.toString());
  }

  @Test
  void exchangingWithoutACodeFailsBeforeReachingTheNetwork() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> RestOAuth2TokenProvider.exchangeAuthorizationCode(clientCredentials(), " ", "v"));

    assertTrue(e.getMessage().contains("authorization code"), e.getMessage());
    assertEquals(0, issued.get());
  }

  @Test
  void anErrorMessageNeverCarriesTheTokensItReceived() {
    // The body has to be shown - it is what names the failure and the granted scope - but it also
    // carries live credentials on a partially successful exchange. These messages end up in error
    // dialogs, log files and issue reports.
    refreshTokenInResponse = null;
    errorBody =
        "{\"token_type\":\"Bearer\",\"scope\":\"api://app/Default_scope\","
            + "\"access_token\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.super-secret-jwt\"}";
    statusCode = 200;

    HopException e =
        assertThrows(
            HopException.class,
            () -> RestOAuth2TokenProvider.exchangeAuthorizationCode(clientCredentials(), "c", "v"));

    String message = e.toString() + (e.getCause() == null ? "" : e.getCause().getMessage());
    assertFalse(message.contains("super-secret-jwt"), "the token must not be in the message");
    assertTrue(message.contains("********"), "it should be visibly redacted: " + message);
    // The diagnosis has to survive the redaction, or the message is useless.
    assertTrue(message.contains("api://app/Default_scope"), message);
  }

  private static boolean causeContains(Throwable t, String needle) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c.getMessage() != null && c.getMessage().contains(needle)) {
        return true;
      }
    }
    return false;
  }

  private RestClientSettings clientCredentials() {
    RestClientSettings settings = new RestClientSettings();
    settings.setAuthType(RestAuthType.OAUTH2);
    settings.setOauth2TokenUrl("http://localhost:" + tokenServer.getAddress().getPort() + "/token");
    settings.setOauth2Grant(RestOAuth2Grant.CLIENT_CREDENTIALS);
    settings.setOauth2ClientId("id");
    settings.setOauth2ClientSecret("secret");
    settings.setOauth2Scope("read things");
    return settings;
  }

  private void issueToken(HttpExchange exchange) throws IOException {
    requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
    authorizationHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));

    if (statusCode != 200) {
      respond(exchange, statusCode, errorBody);
      return;
    }
    if (!errorBody.isEmpty()) {
      respond(exchange, 200, errorBody);
      return;
    }

    String token = "token-" + issued.incrementAndGet();
    String expiry = expiresIn > 0 ? ",\"expires_in\":" + expiresIn : "";
    String refresh =
        refreshTokenInResponse == null
            ? ""
            : ",\"refresh_token\":\"" + refreshTokenInResponse + "\"";
    respond(
        exchange,
        200,
        "{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\"" + expiry + refresh + "}");
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static String decodeBasic(String header) {
    return new String(
        Base64.getDecoder().decode(header.substring("Basic ".length())), StandardCharsets.UTF_8);
  }
}
