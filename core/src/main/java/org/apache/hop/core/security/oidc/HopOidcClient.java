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

package org.apache.hop.core.security.oidc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopRole;
import org.apache.hop.core.security.HopSecurityConfig;
import org.apache.hop.core.security.HopSecurityContext;

/**
 * Lightweight OpenID Connect client: discovery, authorization-code + PKCE, ID-token validation, and
 * role claim extraction. Uses JDK {@link HttpClient}, Jackson, and Nimbus JOSE JWT (already on the
 * Hop classpath).
 */
public final class HopOidcClient {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(20);
  private static final Map<String, OidcDiscoveryDocument> DISCOVERY_CACHE =
      new ConcurrentHashMap<>();

  private final HopSecurityConfig config;
  private final HttpClient httpClient;
  private final ObjectMapper mapper;

  public HopOidcClient(HopSecurityConfig config) {
    this.config = config;
    this.httpClient = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
    this.mapper = HopJson.newMapper();
  }

  public OidcDiscoveryDocument getDiscovery() throws Exception {
    String issuer = trimTrailingSlash(config.getOauthIssuerUrl());
    if (issuer == null || issuer.isBlank()) {
      throw new IllegalStateException("OAuth issuer URL is not configured");
    }
    return DISCOVERY_CACHE.computeIfAbsent(
        issuer,
        key -> {
          try {
            return fetchDiscovery(key);
          } catch (Exception e) {
            throw new IllegalStateException("OIDC discovery failed for issuer " + key, e);
          }
        });
  }

  /** Drop cached discovery (after config change). */
  public static void clearDiscoveryCache() {
    DISCOVERY_CACHE.clear();
  }

  private OidcDiscoveryDocument fetchDiscovery(String issuer) throws Exception {
    String url = issuer + "/.well-known/openid-configuration";
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(url)).timeout(HTTP_TIMEOUT).GET().build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new IllegalStateException(
          "OIDC discovery HTTP " + response.statusCode() + " from " + url);
    }
    OidcDiscoveryDocument doc = mapper.readValue(response.body(), OidcDiscoveryDocument.class);
    if (doc.getAuthorizationEndpoint() == null || doc.getTokenEndpoint() == null) {
      throw new IllegalStateException("OIDC discovery missing authorization/token endpoints");
    }
    LogChannel.GENERAL.logBasic("OIDC discovery loaded for issuer ''{0}''", issuer);
    return doc;
  }

  public String buildAuthorizationUrl(
      String redirectUri, String state, String nonce, String codeChallenge) throws Exception {
    OidcDiscoveryDocument doc = getDiscovery();
    String scopes =
        config.getOauthScopes() == null || config.getOauthScopes().isBlank()
            ? "openid profile email"
            : config.getOauthScopes().trim();
    StringBuilder url = new StringBuilder(doc.getAuthorizationEndpoint());
    url.append(doc.getAuthorizationEndpoint().contains("?") ? "&" : "?");
    url.append("response_type=code");
    url.append("&client_id=").append(enc(config.getOauthClientId()));
    url.append("&redirect_uri=").append(enc(redirectUri));
    url.append("&scope=").append(enc(scopes));
    url.append("&state=").append(enc(state));
    url.append("&nonce=").append(enc(nonce));
    if (config.isOauthUsePkce() && codeChallenge != null) {
      url.append("&code_challenge=").append(enc(codeChallenge));
      url.append("&code_challenge_method=S256");
    }
    return url.toString();
  }

  public OidcTokenResponse exchangeCode(String code, String redirectUri, String codeVerifier)
      throws Exception {
    OidcDiscoveryDocument doc = getDiscovery();
    StringBuilder body = new StringBuilder();
    body.append("grant_type=authorization_code");
    body.append("&code=").append(enc(code));
    body.append("&redirect_uri=").append(enc(redirectUri));
    body.append("&client_id=").append(enc(config.getOauthClientId()));
    String secret = config.resolveOauthClientSecret();
    if (secret != null && !secret.isBlank()) {
      body.append("&client_secret=").append(enc(secret));
    }
    if (config.isOauthUsePkce() && codeVerifier != null && !codeVerifier.isBlank()) {
      body.append("&code_verifier=").append(enc(codeVerifier));
    }

    HttpRequest request =
        HttpRequest.newBuilder(URI.create(doc.getTokenEndpoint()))
            .timeout(HTTP_TIMEOUT)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    OidcTokenResponse token = mapper.readValue(response.body(), OidcTokenResponse.class);
    if (response.statusCode() < 200 || response.statusCode() >= 300 || token.getError() != null) {
      String msg =
          token.getErrorDescription() != null
              ? token.getErrorDescription()
              : (token.getError() != null ? token.getError() : "HTTP " + response.statusCode());
      throw new IllegalStateException("Token exchange failed: " + msg);
    }
    if (token.getIdToken() == null || token.getIdToken().isBlank()) {
      throw new IllegalStateException("Token response did not include an id_token");
    }
    return token;
  }

  /**
   * Validate ID token signature (JWKS) and return claims. Validates issuer when present in
   * discovery.
   */
  public JWTClaimsSet validateIdToken(String idToken, String expectedNonce) throws Exception {
    OidcDiscoveryDocument doc = getDiscovery();
    if (doc.getJwksUri() == null || doc.getJwksUri().isBlank()) {
      throw new IllegalStateException("OIDC discovery missing jwks_uri");
    }
    ConfigurableJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
    JWKSource<SecurityContext> keySource = new RemoteJWKSet<>(new URL(doc.getJwksUri()));
    Set<JWSAlgorithm> algs =
        Set.of(
            JWSAlgorithm.RS256,
            JWSAlgorithm.RS384,
            JWSAlgorithm.RS512,
            JWSAlgorithm.ES256,
            JWSAlgorithm.ES384,
            JWSAlgorithm.ES512,
            JWSAlgorithm.PS256,
            JWSAlgorithm.PS384,
            JWSAlgorithm.PS512);
    JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(algs, keySource);
    processor.setJWSKeySelector(keySelector);
    JWTClaimsSet claims = processor.process(idToken, null);

    String issuer = trimTrailingSlash(config.getOauthIssuerUrl());
    if (claims.getIssuer() != null
        && issuer != null
        && !issuer.equals(trimTrailingSlash(claims.getIssuer()))) {
      throw new IllegalStateException(
          "ID token issuer mismatch: " + claims.getIssuer() + " vs " + issuer);
    }
    if (expectedNonce != null && !expectedNonce.isBlank()) {
      Object nonce = claims.getClaim("nonce");
      if (nonce == null || !expectedNonce.equals(String.valueOf(nonce))) {
        throw new IllegalStateException("ID token nonce mismatch");
      }
    }
    // Audience: must include our client id when present
    List<String> aud = claims.getAudience();
    if (aud != null
        && !aud.isEmpty()
        && config.getOauthClientId() != null
        && !aud.contains(config.getOauthClientId())) {
      throw new IllegalStateException("ID token audience does not include client_id");
    }
    return claims;
  }

  public HopSecurityContext toSecurityContext(JWTClaimsSet claims) {
    String username = extractUsername(claims);
    Set<String> roleNames = new LinkedHashSet<>(extractRoleNames(claims));
    // Also map username / email via roleMappings (e.g. "you@gmail.com" → admin for Google OAuth
    // where there is no groups claim)
    if (username != null && !username.isBlank()) {
      roleNames.add(username.trim());
    }
    Object emailClaim = claims.getClaim("email");
    if (emailClaim != null && !String.valueOf(emailClaim).isBlank()) {
      roleNames.add(String.valueOf(emailClaim).trim());
    }
    Set<HopRole> hopRoles = new LinkedHashSet<>();
    for (String roleName : roleNames) {
      HopRole mapped = config.mapContainerRole(roleName);
      if (mapped != null) {
        hopRoles.add(mapped);
      }
    }
    if (hopRoles.isEmpty()) {
      hopRoles.add(HopRole.USER);
    }
    // Retain claim group names so project access rules can match IdP/LDAP groups
    return HopSecurityContext.forUser(username, hopRoles, roleNames);
  }

  public Set<String> expandRolesForPrincipal(JWTClaimsSet claims) {
    Set<String> names = extractRoleNames(claims);
    Set<String> expanded = new LinkedHashSet<>(names);
    for (String name : names) {
      HopRole role = config.mapContainerRole(name);
      if (role != null) {
        expanded.add(role.getId());
        expanded.add("hop-" + role.getId());
      }
    }
    return expanded;
  }

  public String extractUsername(JWTClaimsSet claims) {
    String claim =
        config.getOauthUsernameClaim() == null || config.getOauthUsernameClaim().isBlank()
            ? "preferred_username"
            : config.getOauthUsernameClaim().trim();
    Object value = claims.getClaim(claim);
    if (value != null && !String.valueOf(value).isBlank()) {
      return String.valueOf(value).trim();
    }
    value = claims.getClaim("email");
    if (value != null && !String.valueOf(value).isBlank()) {
      return String.valueOf(value).trim();
    }
    if (claims.getSubject() != null && !claims.getSubject().isBlank()) {
      return claims.getSubject().trim();
    }
    return "oidc-user";
  }

  public Set<String> extractRoleNames(JWTClaimsSet claims) {
    String path =
        config.getOauthRoleClaim() == null || config.getOauthRoleClaim().isBlank()
            ? "groups"
            : config.getOauthRoleClaim().trim();
    Set<String> roles = new LinkedHashSet<>();
    try {
      // Convert claims map to JsonNode for dotted path support
      JsonNode root = mapper.valueToTree(claims.toJSONObject());
      JsonNode node = resolvePath(root, path);
      if (node == null || node.isNull()) {
        return roles;
      }
      if (node.isArray()) {
        for (JsonNode item : node) {
          if (item != null && !item.isNull() && !item.asText().isBlank()) {
            roles.add(item.asText().trim());
          }
        }
      } else if (node.isTextual()) {
        // space or comma separated
        for (String part : node.asText().split("[,\\s]+")) {
          if (!part.isBlank()) {
            roles.add(part.trim());
          }
        }
      } else if (node.isObject()) {
        // e.g. resource_access.hop.roles as object of flags — take field names that are true
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> e = fields.next();
          if (e.getValue() != null && e.getValue().asBoolean(false)) {
            roles.add(e.getKey());
          }
        }
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Failed to extract OIDC roles from claim ''{0}''", path, e);
    }
    return roles;
  }

  public String buildEndSessionUrl(String idToken, String postLogoutRedirectUri) throws Exception {
    String endpoint = config.getOauthEndSessionEndpoint();
    if (endpoint == null || endpoint.isBlank()) {
      OidcDiscoveryDocument doc = getDiscovery();
      endpoint = doc.getEndSessionEndpoint();
    }
    if (endpoint == null || endpoint.isBlank()) {
      return null;
    }
    StringBuilder url = new StringBuilder(endpoint);
    url.append(endpoint.contains("?") ? "&" : "?");
    if (idToken != null && !idToken.isBlank()) {
      url.append("id_token_hint=").append(enc(idToken));
      url.append("&");
    }
    if (postLogoutRedirectUri != null && !postLogoutRedirectUri.isBlank()) {
      url.append("post_logout_redirect_uri=").append(enc(postLogoutRedirectUri));
    }
    return url.toString();
  }

  public static String newState() {
    return randomUrlToken(24);
  }

  public static String newNonce() {
    return randomUrlToken(24);
  }

  public static String newCodeVerifier() {
    return randomUrlToken(32);
  }

  public static String codeChallengeS256(String verifier) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static String randomUrlToken(int bytes) {
    byte[] buf = new byte[bytes];
    RANDOM.nextBytes(buf);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
  }

  private static JsonNode resolvePath(JsonNode root, String path) {
    if (root == null || path == null) {
      return null;
    }
    JsonNode current = root;
    for (String part : path.split("\\.")) {
      if (current == null || current.isNull()) {
        return null;
      }
      current = current.get(part);
    }
    return current;
  }

  private static String enc(String value) {
    return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
  }

  private static String trimTrailingSlash(String url) {
    if (url == null) {
      return null;
    }
    String u = url.trim();
    while (u.endsWith("/")) {
      u = u.substring(0, u.length() - 1);
    }
    return u;
  }

  /** Session payload stored between /oauth/start and /oauth/callback. */
  public static final class AuthSession {
    public String state;
    public String nonce;
    public String codeVerifier;
    public String redirectAfterLogin;
    public String idToken; // retained for logout

    public AuthSession() {}
  }
}
