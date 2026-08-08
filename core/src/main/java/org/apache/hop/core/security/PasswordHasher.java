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

package org.apache.hop.core.security;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * One-way password hashing for Hop-managed BASIC authentication. Format:
 *
 * <pre>
 * pbkdf2$&lt;iterations&gt;$&lt;salt-base64&gt;$&lt;hash-base64&gt;
 * </pre>
 *
 * Uses PBKDF2-HMAC-SHA256. Not suitable for reversible credential storage (use the Hop password
 * encoder plugins for that).
 */
public final class PasswordHasher {

  public static final String ALGORITHM_PREFIX = "pbkdf2";
  public static final int DEFAULT_ITERATIONS = 65_536;
  public static final int SALT_BYTES = 16;
  public static final int KEY_BITS = 256;

  private static final SecureRandom RANDOM = new SecureRandom();

  private PasswordHasher() {
    // utility
  }

  /**
   * Hash a clear-text password with a random salt.
   *
   * @param clearPassword clear-text password
   * @return encoded hash string
   */
  public static String hash(String clearPassword) {
    if (clearPassword == null) {
      throw new IllegalArgumentException("password is null");
    }
    byte[] salt = new byte[SALT_BYTES];
    RANDOM.nextBytes(salt);
    return hash(clearPassword, salt, DEFAULT_ITERATIONS);
  }

  /**
   * Verify a clear-text password against a stored hash.
   *
   * @param clearPassword clear-text password
   * @param storedHash value from {@link #hash(String)}
   * @return true if the password matches
   */
  public static boolean verify(String clearPassword, String storedHash) {
    if (clearPassword == null || storedHash == null || storedHash.isBlank()) {
      return false;
    }
    String[] parts = storedHash.split("\\$");
    if (parts.length != 4 || !ALGORITHM_PREFIX.equals(parts[0])) {
      return false;
    }
    try {
      int iterations = Integer.parseInt(parts[1]);
      byte[] salt = Base64.getDecoder().decode(parts[2]);
      byte[] expected = Base64.getDecoder().decode(parts[3]);
      byte[] actual = pbkdf2(clearPassword, salt, iterations);
      return MessageDigest.isEqual(expected, actual);
    } catch (Exception e) {
      return false;
    }
  }

  static String hash(String clearPassword, byte[] salt, int iterations) {
    try {
      byte[] hash = pbkdf2(clearPassword, salt, iterations);
      return ALGORITHM_PREFIX
          + "$"
          + iterations
          + "$"
          + Base64.getEncoder().encodeToString(salt)
          + "$"
          + Base64.getEncoder().encodeToString(hash);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Unable to hash password", e);
    }
  }

  private static byte[] pbkdf2(String password, byte[] salt, int iterations)
      throws GeneralSecurityException {
    PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, KEY_BITS);
    SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
    try {
      return factory.generateSecret(spec).getEncoded();
    } finally {
      spec.clearPassword();
    }
  }

  /**
   * Constant-time-ish empty check used by callers that only need to know if a hash string looks
   * well-formed.
   *
   * @param storedHash stored hash
   * @return true if the format is recognized
   */
  public static boolean isHashedFormat(String storedHash) {
    if (storedHash == null) {
      return false;
    }
    String[] parts = storedHash.split("\\$");
    return parts.length == 4 && ALGORITHM_PREFIX.equals(parts[0]);
  }
}
