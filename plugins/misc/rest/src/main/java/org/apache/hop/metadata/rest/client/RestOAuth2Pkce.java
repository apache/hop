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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

/**
 * PKCE (RFC 7636) for the interactive authorization flow.
 *
 * <p>The authorization code travels through a browser and, with a loopback or custom-scheme
 * redirect, can be observed by other software on the machine. PKCE makes an intercepted code
 * useless on its own: only the client holding the original verifier can redeem it. It is required
 * for public clients and recommended for all of them, so it is not optional here.
 *
 * @param codeVerifier the secret kept by Hop until the code is exchanged
 * @param codeChallenge the SHA-256 hash sent to the authorization server
 */
public record RestOAuth2Pkce(String codeVerifier, String codeChallenge) {

  /** A fresh verifier and its challenge. Generate one per authorization attempt, never reuse. */
  public static RestOAuth2Pkce generate() throws HopException {
    // A new SecureRandom per authorization: these happen once, by hand, so pooling gains nothing.
    @SuppressWarnings("java:S2119")
    SecureRandom random = new SecureRandom();
    byte[] bytes = new byte[32];
    random.nextBytes(bytes);
    String verifier = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);

    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.UTF_8));
      return new RestOAuth2Pkce(
          verifier, Base64.getUrlEncoder().withoutPadding().encodeToString(hash));
    } catch (NoSuchAlgorithmException e) {
      throw new HopException("SHA-256 is required for PKCE but is not available", e);
    }
  }

  /**
   * The URL to open in a browser to ask the user for consent.
   *
   * @param settings supplies the authorization endpoint, client id, redirect URI and scope
   * @param forceConsent add {@code prompt=consent}; some servers only return a refresh token when
   *     the user is asked again, so this is the escape hatch when the exchange comes back without
   *     one
   */
  public String authorizationUrl(RestClientSettings settings, boolean forceConsent)
      throws HopException {
    if (Utils.isEmpty(settings.getOauth2AuthorizationUrl())) {
      throw new HopException("An authorization URL is required to start the authorize flow");
    }
    if (Utils.isEmpty(settings.getOauth2ClientId())) {
      throw new HopException("A client ID is required to start the authorize flow");
    }

    StringBuilder url = new StringBuilder(settings.getOauth2AuthorizationUrl().trim());
    url.append(url.indexOf("?") < 0 ? '?' : '&');
    url.append("response_type=code");
    append(url, "client_id", settings.getOauth2ClientId());
    if (!Utils.isEmpty(settings.getOauth2RedirectUri())) {
      append(url, "redirect_uri", settings.getOauth2RedirectUri());
    }
    if (!Utils.isEmpty(settings.getOauth2Scope())) {
      append(url, "scope", settings.getOauth2Scope());
    }
    append(url, "code_challenge", codeChallenge);
    append(url, "code_challenge_method", "S256");
    if (forceConsent) {
      append(url, "prompt", "consent");
    }
    return url.toString();
  }

  private static void append(StringBuilder url, String name, String value) {
    url.append('&')
        .append(name)
        .append('=')
        .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
  }
}
