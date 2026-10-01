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

package org.apache.hop.git.provider;

import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.StringUtil;

class GitApiHttp {

  static final String USER_AGENT = "Apache-Hop-GitInput";

  /**
   * Uses the JVM's default proxy selector so the standard {@code http.proxyHost} / {@code
   * https.proxyHost} system properties are honoured. Self-hosted GitLab, Gitea and Forgejo
   * instances commonly sit behind a corporate proxy. TLS trust follows the usual {@code
   * javax.net.ssl.trustStore} properties, which the JDK client picks up on its own.
   */
  private static final HttpClient HTTP =
      HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(10))
          .proxy(ProxySelector.getDefault())
          .build();

  private static final int MAX_RETRIES = 3;
  private static final long RETRY_BASE_MS = 1000L;

  /** Ceiling on a single backoff, so a far-future reset header cannot stall a pipeline. */
  private static final long MAX_RETRY_DELAY_MS = 60_000L;

  static final class ApiResponse {
    private final String body;
    private final boolean hasNextPage;

    ApiResponse(String body, boolean hasNextPage) {
      this.body = body;
      this.hasNextPage = hasNextPage;
    }

    String getBody() {
      return body;
    }

    boolean isHasNextPage() {
      return hasNextPage;
    }
  }

  private GitApiHttp() {}

  static String trimBase(String apiBaseUrl) {
    return StringUtil.trimEnd(apiBaseUrl, '/');
  }

  static String get(
      String url,
      GitAuth auth,
      GitProvider.AuthStyle authStyle,
      String acceptHeader,
      String providerLabel)
      throws HopException {
    return get(url, auth, authStyle, acceptHeader, providerLabel, null);
  }

  static String get(
      String url,
      GitAuth auth,
      GitProvider.AuthStyle authStyle,
      String acceptHeader,
      String providerLabel,
      java.util.Map<String, String> extraHeaders)
      throws HopException {
    return getWithPagination(url, auth, authStyle, acceptHeader, providerLabel, extraHeaders)
        .getBody();
  }

  static ApiResponse getWithPagination(
      String url,
      GitAuth auth,
      GitProvider.AuthStyle authStyle,
      String acceptHeader,
      String providerLabel,
      java.util.Map<String, String> extraHeaders)
      throws HopException {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .header("Accept", acceptHeader)
            .header("User-Agent", USER_AGENT)
            .GET();
    if (extraHeaders != null) {
      for (java.util.Map.Entry<String, String> entry : extraHeaders.entrySet()) {
        builder.header(entry.getKey(), entry.getValue());
      }
    }
    applyAuth(builder, auth, authStyle);

    HopException lastError = null;
    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        HttpResponse<String> response =
            HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        int statusCode = response.statusCode();
        if (statusCode == 200) {
          boolean hasNextPage =
              response
                  .headers()
                  .firstValue("Link")
                  .map(GitApiHttp::linkHeaderHasNext)
                  .orElse(false);
          return new ApiResponse(response.body(), hasNextPage);
        }
        if (statusCode == 401) {
          throw new HopException(
              providerLabel
                  + " authentication failed (HTTP 401). Check the token or credentials, unresolved"
                  + " variables, and required API scopes.");
        }
        if (statusCode == 403 && GitApiException.isRateLimited(response)) {
          if (attempt < MAX_RETRIES) {
            sleepBeforeRetry(attempt, retryDelayMs(response, attempt));
            continue;
          }
          throw new HopException(
              providerLabel
                  + " rate limit exceeded (HTTP 403) and it did not reset within the retry window."
                  + " Give the connection a token for a higher limit, or narrow the run with Since,"
                  + " a smaller page size, or fewer max pages.");
        }
        if (statusCode == 403) {
          String detail = summarizeErrorBody(response.body(), providerLabel);
          throw new HopException(
              providerLabel
                  + " access denied (HTTP 403)."
                  + detail
                  + " Check token scopes (Issues: Read), repository access, org SSO authorization,"
                  + " and rate limits.");
        }
        GitApiException apiError = new GitApiException(providerLabel, statusCode, url);
        if (apiError.isRetryable() && attempt < MAX_RETRIES) {
          sleepBeforeRetry(attempt, retryDelayMs(response, attempt));
          continue;
        }
        throw apiError;
      } catch (InterruptedException e) {
        // A stopped pipeline must not be retried into a multi-second sleep.
        Thread.currentThread().interrupt();
        throw new HopException(
            "Interrupted while calling the " + providerLabel + " API. URL: " + url, e);
      } catch (IOException e) {
        lastError =
            new HopException(
                "Failed to connect to " + providerLabel + " API: " + e.getMessage(), e);
        if (attempt < MAX_RETRIES) {
          sleepBeforeRetry(attempt, backoffMs(attempt));
          continue;
        }
        throw lastError;
      }
    }
    throw lastError != null
        ? lastError
        : new HopException("Failed to connect to " + providerLabel + " API. URL: " + url);
  }

  private static boolean linkHeaderHasNext(String linkHeader) {
    if (linkHeader == null || linkHeader.isBlank()) {
      return false;
    }
    for (String part : linkHeader.split(",")) {
      if (part.contains("rel=\"next\"") || part.contains("rel=next")) {
        return true;
      }
    }
    return false;
  }

  private static String summarizeErrorBody(String body, String providerLabel) {
    if (body == null || body.isBlank()) {
      return "";
    }
    try {
      Object parsed = new org.json.simple.parser.JSONParser().parse(body);
      // An error body is usually an object, but not always: a bare array or string must not throw
      // out of the error path and replace the message the caller needs.
      if (parsed instanceof org.json.simple.JSONObject json) {
        String message = getString(json, "message");
        if (!message.isBlank()) {
          return " " + providerLabel + " says: " + message;
        }
      }
    } catch (Exception e) {
      // Not JSON, or not a shape we can read: fall through to no detail.
    }
    return "";
  }

  private static long backoffMs(int attempt) {
    return RETRY_BASE_MS * (1L << attempt);
  }

  /**
   * Prefers the provider's own guidance over blind exponential backoff. GitHub answers a tripped
   * rate limit with {@code Retry-After} (seconds) or an {@code X-RateLimit-Reset} epoch second;
   * plain exponential backoff tops out around eight seconds and would simply burn the remaining
   * retries against a limit that has not lifted yet.
   */
  static long retryDelayMs(HttpResponse<String> response, int attempt) {
    long fallback = backoffMs(attempt);
    long retryAfter =
        response.headers().firstValue("Retry-After").map(GitApiHttp::parseSeconds).orElse(-1L);
    if (retryAfter >= 0) {
      return Math.min(Math.max(retryAfter * 1000L, fallback), MAX_RETRY_DELAY_MS);
    }
    if (response.statusCode() == 403 || response.statusCode() == 429) {
      long reset =
          response
              .headers()
              .firstValue("X-RateLimit-Reset")
              .map(GitApiHttp::parseSeconds)
              .orElse(-1L);
      if (reset > 0) {
        long waitMs = (reset * 1000L) - System.currentTimeMillis();
        if (waitMs > 0) {
          return Math.min(Math.max(waitMs, fallback), MAX_RETRY_DELAY_MS);
        }
      }
    }
    return fallback;
  }

  private static long parseSeconds(String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  private static void sleepBeforeRetry(int attempt, long delayMs) throws HopException {
    try {
      Thread.sleep(delayMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted while retrying Git API request", e);
    }
  }

  private static void applyAuth(
      HttpRequest.Builder builder, GitAuth auth, GitProvider.AuthStyle callerStyle) {
    if (auth == null) {
      return;
    }
    // A credential built from a connection knows which header it belongs in; the caller's style is
    // only a fallback for the legacy factories used in tests.
    GitProvider.AuthStyle authStyle = auth.getStyle() != null ? auth.getStyle() : callerStyle;
    if (authStyle == GitProvider.AuthStyle.BASIC && auth.isBasicAuth()) {
      String credentials =
          Base64.getEncoder()
              .encodeToString(
                  (auth.getUsername() + ":" + auth.getPassword()).getBytes(StandardCharsets.UTF_8));
      builder.header("Authorization", "Basic " + credentials);
    } else if (auth.isTokenBased()) {
      if (authStyle == GitProvider.AuthStyle.GITLAB_PRIVATE_TOKEN) {
        builder.header("PRIVATE-TOKEN", auth.getToken());
      } else if (authStyle == GitProvider.AuthStyle.TOKEN_HEADER) {
        builder.header("Authorization", "token " + auth.getToken());
      } else if (authStyle == GitProvider.AuthStyle.BEARER) {
        builder.header("Authorization", "Bearer " + auth.getToken());
      }
    }
  }

  static String getString(org.json.simple.JSONObject obj, String key) {
    if (obj == null) {
      return "";
    }
    Object val = obj.get(key);
    return val != null ? val.toString() : "";
  }

  static long getLong(org.json.simple.JSONObject obj, String key) {
    Object val = obj == null ? null : obj.get(key);
    if (val instanceof Number number) {
      return number.longValue();
    }
    if (val != null) {
      try {
        return Long.parseLong(val.toString());
      } catch (NumberFormatException ignored) {
        return 0L;
      }
    }
    return 0L;
  }

  static org.json.simple.JSONArray parseArray(String json, String providerLabel)
      throws HopException {
    try {
      return (org.json.simple.JSONArray) new org.json.simple.parser.JSONParser().parse(json);
    } catch (org.json.simple.parser.ParseException e) {
      throw new HopException(
          "Failed to parse " + providerLabel + " API response: " + e.getMessage(), e);
    }
  }

  static org.json.simple.JSONObject parseObject(String json, String providerLabel)
      throws HopException {
    try {
      return (org.json.simple.JSONObject) new org.json.simple.parser.JSONParser().parse(json);
    } catch (org.json.simple.parser.ParseException e) {
      throw new HopException(
          "Failed to parse " + providerLabel + " API response: " + e.getMessage(), e);
    }
  }

  /**
   * Percent-encodes a value for use inside a URL path.
   *
   * <p>{@code URLEncoder} is an HTML form encoder: it turns a space into {@code +}, which a server
   * reads literally in a path segment. Owner and repository names reach the API through here, so
   * the difference is not academic.
   */
  static String urlEncode(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }
}
