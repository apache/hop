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
package org.apache.hop.pipeline.transforms.chunker.document;

import java.util.List;
import java.util.Locale;

/**
 * Keeps credentials out of chunk text.
 *
 * <p>Chunks produced from Hop definitions are normally sent to an embedding model, so a secret
 * copied out of a {@code .hpl}, {@code .hwf} or metadata file would leave the installation.
 */
public final class SecretRedaction {

  public static final String REDACTED = "**redacted**";

  private static final List<String> SECRET_MARKERS =
      List.of(
          "password",
          "passwd",
          "pwd",
          "secret",
          "token",
          "credential",
          "passphrase",
          "privatekey",
          "apikey");

  private SecretRedaction() {}

  /**
   * True when a field or tag name reads like a credential. Names are normalised, so {@code
   * client_secret}, {@code API-KEY} and {@code privateKey} all match. A bare {@code key} does not:
   * it is an ordinary field name in lookups and joins, and redacting it would gut the chunk.
   */
  public static boolean isSecretName(String name) {
    if (name == null) {
      return false;
    }
    String normalised = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    return SECRET_MARKERS.stream().anyMatch(normalised::contains);
  }

  /** True for Hop's own encrypted form, whatever the field is called. */
  public static boolean isEncryptedValue(String value) {
    return value != null && value.stripLeading().startsWith("Encrypted ");
  }

  /** The value to emit for a field, redacted when the name or the value gives it away. */
  public static String redact(String name, String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }
    return isSecretName(name) || isEncryptedValue(value) ? REDACTED : value;
  }
}
