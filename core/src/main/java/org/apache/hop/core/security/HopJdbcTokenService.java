/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.core.security;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Issues and verifies short-lived HMAC-signed JWTs for JDBC / API clients talking to Hop Web.
 *
 * <p>Hop is not an OAuth2 authorization server. These tokens exist so a user who already signed in
 * (BASIC or OIDC) can paste a Bearer credential into a JDBC password field. Signature key is {@code
 * HOP_WEB_JDBC_TOKEN_SECRET} or a generated file under the security folder.
 */
public final class HopJdbcTokenService {

  public static final String ISSUER = "hop-web";
  public static final String AUDIENCE = "hop-jdbc";
  public static final String CLAIM_ROLES = "hop_roles";
  public static final String ENV_SECRET = "HOP_WEB_JDBC_TOKEN_SECRET";
  public static final String SECRET_FILENAME = "jdbc-token.secret";
  public static final Duration DEFAULT_TTL = Duration.ofHours(1);

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int SECRET_BYTES = 32;

  private static volatile byte[] secretOverride;

  private HopJdbcTokenService() {}

  /**
   * Issued token plus expiry for JSON / UI.
   *
   * @param token compact JWT
   * @param expiresAt expiry instant
   */
  public record IssuedToken(String token, Instant expiresAt) {
    public long expiresInSeconds() {
      long seconds = Duration.between(Instant.now(), expiresAt).getSeconds();
      return Math.max(0L, seconds);
    }
  }

  /**
   * Mint a token for this username and role ids.
   *
   * @param username subject
   * @param roles Hop role ids and/or container role names
   * @param ttl lifetime
   * @return signed JWT
   */
  public static IssuedToken issue(String username, Collection<String> roles, Duration ttl)
      throws JOSEException {
    if (username == null || username.isBlank()) {
      throw new IllegalArgumentException("username is required");
    }
    Duration lifetime = ttl == null || ttl.isZero() || ttl.isNegative() ? DEFAULT_TTL : ttl;
    Instant now = Instant.now();
    Instant exp = now.plus(lifetime);
    List<String> roleList = new ArrayList<>();
    if (roles != null) {
      for (String role : roles) {
        if (role != null && !role.isBlank()) {
          roleList.add(role.trim());
        }
      }
    }
    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .subject(username.trim())
            .issueTime(Date.from(now))
            .expirationTime(Date.from(exp))
            .claim(CLAIM_ROLES, roleList)
            .build();
    SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
    jwt.sign(new MACSigner(loadSecret()));
    return new IssuedToken(jwt.serialize(), exp);
  }

  /**
   * Verify signature, issuer, audience and expiry. Throws if the token is not a Hop JDBC token.
   *
   * @param token compact JWT
   * @return claims
   */
  public static JWTClaimsSet verify(String token) throws Exception {
    if (token == null || token.isBlank()) {
      throw new IllegalArgumentException("token is empty");
    }
    SignedJWT jwt = SignedJWT.parse(token.trim());
    if (!jwt.verify(new MACVerifier(loadSecret()))) {
      throw new JOSEException("Hop JDBC token signature is invalid");
    }
    JWTClaimsSet claims = jwt.getJWTClaimsSet();
    if (claims.getExpirationTime() == null || claims.getExpirationTime().before(new Date())) {
      throw new JOSEException("Hop JDBC token has expired");
    }
    if (!ISSUER.equals(claims.getIssuer())) {
      throw new JOSEException("Hop JDBC token issuer mismatch");
    }
    List<String> aud = claims.getAudience();
    if (aud == null || !aud.contains(AUDIENCE)) {
      throw new JOSEException("Hop JDBC token audience mismatch");
    }
    if (claims.getSubject() == null || claims.getSubject().isBlank()) {
      throw new JOSEException("Hop JDBC token has no subject");
    }
    return claims;
  }

  /**
   * Whether {@code token} looks like and verifies as a Hop JDBC token. Invalid signatures return
   * false rather than throwing so callers can fall through to IdP JWT validation.
   *
   * @param token compact JWT
   * @return true when verify succeeds
   */
  public static boolean isHopJdbcToken(String token) {
    try {
      verify(token);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Role names stored in the token (may be empty).
   *
   * @param claims verified claims
   * @return role names
   */
  @SuppressWarnings("unchecked")
  public static Set<String> roleNames(JWTClaimsSet claims) {
    Set<String> roles = new LinkedHashSet<>();
    if (claims == null) {
      return roles;
    }
    Object raw = claims.getClaim(CLAIM_ROLES);
    if (raw instanceof Collection<?> collection) {
      for (Object item : collection) {
        if (item != null && !String.valueOf(item).isBlank()) {
          roles.add(String.valueOf(item).trim());
        }
      }
    }
    return roles;
  }

  /** Visible for tests: pin the HMAC secret so tests do not touch the config folder. */
  static void overrideSecretForTests(byte[] secret) {
    if (secret != null && secret.length < SECRET_BYTES) {
      throw new IllegalArgumentException("HMAC secret must be at least " + SECRET_BYTES + " bytes");
    }
    secretOverride = secret;
  }

  private static byte[] loadSecret() {
    byte[] override = secretOverride;
    if (override != null) {
      return override;
    }
    String env = firstNonBlank(System.getenv(ENV_SECRET), System.getProperty(ENV_SECRET));
    if (env != null) {
      byte[] decoded = decodeSecret(env);
      if (decoded.length >= SECRET_BYTES) {
        return decoded;
      }
      // Treat a passphrase as UTF-8 and reject if too short after encoding.
      decoded = env.getBytes(StandardCharsets.UTF_8);
      if (decoded.length >= SECRET_BYTES) {
        return decoded;
      }
      throw new IllegalStateException(
          ENV_SECRET + " must be at least " + SECRET_BYTES + " bytes (or base64 of that)");
    }
    return loadOrCreateFileSecret();
  }

  private static byte[] loadOrCreateFileSecret() {
    String path =
        Const.HOP_CONFIG_FOLDER
            + Const.FILE_SEPARATOR
            + HopSecurityConfig.SECURITY_FOLDER
            + Const.FILE_SEPARATOR
            + SECRET_FILENAME;
    try {
      if (HopVfs.fileExists(path)) {
        try (InputStream in = HopVfs.getInputStream(path)) {
          String text = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
          byte[] decoded = decodeSecret(text);
          if (decoded.length >= SECRET_BYTES) {
            return decoded;
          }
        }
      }
      byte[] generated = new byte[SECRET_BYTES];
      RANDOM.nextBytes(generated);
      String folder =
          Const.HOP_CONFIG_FOLDER + Const.FILE_SEPARATOR + HopSecurityConfig.SECURITY_FOLDER;
      var folderObject = HopVfs.getFileObject(folder);
      if (!folderObject.exists()) {
        folderObject.createFolder();
      }
      String encoded = Base64.getEncoder().encodeToString(generated);
      try (OutputStream out = HopVfs.getOutputStream(path, false)) {
        out.write(encoded.getBytes(StandardCharsets.UTF_8));
      }
      LogChannel.GENERAL.logBasic("Created Hop JDBC token secret at '" + path + "'");
      return generated;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to load or create JDBC token secret at '" + path + "'", e);
    }
  }

  private static byte[] decodeSecret(String text) {
    try {
      return Base64.getDecoder().decode(text);
    } catch (IllegalArgumentException e) {
      return text.getBytes(StandardCharsets.UTF_8);
    }
  }

  private static String firstNonBlank(String a, String b) {
    if (a != null && !a.isBlank()) {
      return a.trim();
    }
    if (b != null && !b.isBlank()) {
      return b.trim();
    }
    return null;
  }
}
