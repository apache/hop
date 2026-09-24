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

package org.apache.hop.core.util;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Keeps credentials out of log lines, error messages and lineage events about HTTP requests.
 *
 * <p>A URL can carry a user name and password ({@code https://user:secret@host/}) or a key in its
 * query ({@code ?api_key=...}, a pre-signed {@code X-Amz-Signature}, an Azure SAS {@code sig});
 * headers and request bodies carry tokens and passwords. All of that is written to the log at one
 * level or another, and logs end up in tickets, CI output and log aggregators. The values are
 * replaced with {@link #MASK}; the names stay, so a line still says what was sent.
 *
 * <p>Whether a value is a credential is decided by its name, see {@link #isSensitiveName(String)}.
 * That errs on the side of masking: a harmless value under a name like {@code nextPageToken} is
 * masked too.
 */
public final class CredentialRedactor {

  /** What a credential is replaced with. */
  public static final String MASK = "********";

  /** The user info of a URL: everything between {@code ://} and an {@code @} in the authority. */
  private static final Pattern USER_INFO = Pattern.compile("(?<=://)[^/?#@\\s]+@");

  /** A {@code name=value} pair, as found in a query string, a fragment or a form body. */
  private static final Pattern KEY_VALUE =
      Pattern.compile("([A-Za-z0-9_.%\\-\\[\\]]+)=([^&#;\\s\"',]*)");

  /** A {@code "name": "value"} pair in JSON. */
  private static final Pattern JSON_PAIR =
      Pattern.compile("(\"((?:[^\"\\\\]|\\\\.){1,200})\"\\s*:\\s*\")((?:[^\"\\\\]|\\\\.)*)(\")");

  /** The credentials of an {@code Authorization} header value quoted somewhere in a text. */
  private static final Pattern AUTH_SCHEME =
      Pattern.compile("(?i)\\b(Bearer|Basic)\\s+[A-Za-z0-9._~+/=\\-]{8,}");

  /** Names, once normalized, that are credentials as they stand. */
  private static final Set<String> SENSITIVE_NAMES =
      Set.of("auth", "cookie", "setcookie", "key", "sig", "session", "sid", "jsessionid");

  /** Parts of a normalized name that make it a credential. */
  private static final String[] SENSITIVE_PARTS = {
    "authoriz", "token", "secret", "passw", "pwd", "credential", "signature", "sessionid"
  };

  /** Endings of a normalized name that make it a credential. */
  private static final String[] SENSITIVE_ENDINGS = {
    "apikey", "accesskey", "secretkey", "privatekey", "subscriptionkey", "masterkey", "signingkey"
  };

  private CredentialRedactor() {}

  /**
   * Whether a header, query parameter or field of this name holds a credential. Case, dashes and
   * underscores do not matter: {@code X-Api-Key}, {@code api_key} and {@code apiKey} are the same.
   */
  public static boolean isSensitiveName(String name) {
    if (name == null) {
      return false;
    }
    String normalized = normalize(name);
    if (normalized.isEmpty()) {
      return false;
    }
    if (SENSITIVE_NAMES.contains(normalized)) {
      return true;
    }
    for (String part : SENSITIVE_PARTS) {
      if (normalized.contains(part)) {
        return true;
      }
    }
    for (String ending : SENSITIVE_ENDINGS) {
      if (normalized.endsWith(ending)) {
        return true;
      }
    }
    return false;
  }

  /**
   * The value of a header, parameter or field as it may be logged: masked entirely when the name
   * says it is a credential, otherwise with any credentials inside it masked (see {@link
   * #redact(String)}).
   */
  public static String redactValue(String name, String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }
    return isSensitiveName(name) ? MASK : redact(value);
  }

  /**
   * A text as it may be logged: a URL, a request or response body, an exception message. The user
   * info of every URL in it is masked, as are the values of {@code name=value} and JSON {@code
   * "name": "value"} pairs whose name is a credential, and the credentials of a {@code Bearer} or
   * {@code Basic} authorization.
   */
  public static String redact(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }
    String result = USER_INFO.matcher(text).replaceAll(Matcher.quoteReplacement(MASK + "@"));
    result = replacePairs(KEY_VALUE, result, m -> m.group(1), m -> m.group(1) + "=" + MASK);
    result = replacePairs(JSON_PAIR, result, m -> m.group(2), m -> m.group(1) + MASK + m.group(4));
    return AUTH_SCHEME.matcher(result).replaceAll("$1 " + Matcher.quoteReplacement(MASK));
  }

  private static String replacePairs(
      Pattern pattern,
      String text,
      Function<Matcher, String> name,
      Function<Matcher, String> masked) {
    Matcher matcher = pattern.matcher(text);
    StringBuilder result = new StringBuilder(text.length());
    while (matcher.find()) {
      String replacement =
          isSensitiveName(decode(name.apply(matcher))) ? masked.apply(matcher) : matcher.group();
      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String decode(String name) {
    try {
      return URLDecoder.decode(name, StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return name;
    }
  }

  private static String normalize(String name) {
    StringBuilder normalized = new StringBuilder(name.length());
    for (char c : name.toLowerCase(Locale.ROOT).toCharArray()) {
      if (Character.isLetterOrDigit(c)) {
        normalized.append(c);
      }
    }
    return normalized.toString();
  }
}
