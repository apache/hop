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
import java.util.Set;
import java.util.regex.Pattern;

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
          "apikey",
          // The REST connection's API key header value (auth_header_value).
          "authheadervalue");

  /**
   * Prefixes of the AES password encoders in the passwords plugin. Unlike "Encrypted ", these are
   * short words, so the rest of the value has to look like the encoder's base64 as well.
   */
  private static final List<String> ENCODER_PREFIXES = List.of("AES ", "AES2 ");

  private static final Pattern ENCODED_BODY = Pattern.compile("[A-Za-z0-9+/=]{16,}");

  private static final Pattern VARIABLE_REFERENCE = Pattern.compile("\\$\\{[^}]+}");

  /**
   * Keys that hold the value of a name/value entry: a variable or action parameter ({@code value}),
   * a parameter default ({@code default_value}), a Set variables field ({@code variable_value}) or
   * a pipeline executor mapping ({@code input}). Which key holds the entry's name differs between
   * Hop XML and metadata JSON, so each parser decides that.
   */
  private static final Set<String> ENTRY_VALUE_KEYS =
      Set.of("value", "defaultvalue", "variablevalue", "input");

  private SecretRedaction() {}

  private static String normalise(String name) {
    return name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
  }

  /**
   * True when a field or tag name reads like a credential. Names are normalised, so {@code
   * client_secret}, {@code API-KEY} and {@code privateKey} all match. A bare {@code key} does not:
   * it is an ordinary field name in lookups and joins, and redacting it would gut the chunk.
   */
  public static boolean isSecretName(String name) {
    if (name == null) {
      return false;
    }
    String normalised = normalise(name);
    return SECRET_MARKERS.stream().anyMatch(normalised::contains);
  }

  /**
   * True when the name of a name/value entry, such as a variable, a parameter or an HTTP header,
   * reads like a credential. Besides the field name markers this matches {@code Authorization} and
   * {@code Proxy-Authorization} headers. That marker is not used for field names: {@code
   * oauth2_authorization_url} holds a URL, not a secret.
   */
  public static boolean isSecretEntryName(String name) {
    return isSecretName(name) || (name != null && normalise(name).contains("authorization"));
  }

  /**
   * True when {@code key} holds the value of a name/value entry, as in {@code {"name": "API_TOKEN",
   * "value": "..."}}. The field holding the value is called {@code value}, so the entry's name has
   * to be checked instead.
   */
  public static boolean isEntryValueKey(String key) {
    return key != null && ENTRY_VALUE_KEYS.contains(normalise(key));
  }

  /**
   * True for an encoded password, whatever the field is called: Hop's own {@code Encrypted} form or
   * that of the AES encoders.
   */
  public static boolean isEncryptedValue(String value) {
    if (value == null) {
      return false;
    }
    String trimmed = value.strip();
    if (trimmed.startsWith("Encrypted ")) {
      return true;
    }
    for (String prefix : ENCODER_PREFIXES) {
      if (trimmed.startsWith(prefix)
          && ENCODED_BODY.matcher(trimmed.substring(prefix.length())).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when the value is only a variable reference, such as {@code ${PROD_DB_PASSWORD}}. That
   * names where a credential comes from, which is useful to find, and is not the credential.
   */
  public static boolean isVariableReference(String value) {
    return value != null && VARIABLE_REFERENCE.matcher(value.strip()).matches();
  }

  /** A value that is known to hold a credential: redacted, unless empty or a variable reference. */
  public static String redactValue(String value) {
    if (value == null || value.isEmpty() || isVariableReference(value)) {
      return value;
    }
    return REDACTED;
  }

  /** The value to emit for a field, redacted when the name or the value gives it away. */
  public static String redact(String name, String value) {
    return isSecretName(name) || isEncryptedValue(value) ? redactValue(value) : value;
  }

  /** The value of a name/value entry, redacted when the entry's name or the value gives it away. */
  public static String redactEntryValue(String entryName, String value) {
    return isSecretEntryName(entryName) ? redactValue(value) : redact(null, value);
  }
}
