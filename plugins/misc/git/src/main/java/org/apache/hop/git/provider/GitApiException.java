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

import org.apache.hop.core.exception.HopException;

/** Thrown when a Git provider REST API returns a non-success HTTP status. */
class GitApiException extends HopException {

  private final int statusCode;
  private final String url;

  GitApiException(String providerLabel, int statusCode, String url) {
    super(providerLabel + " API returned HTTP " + statusCode + ". URL: " + url);
    this.statusCode = statusCode;
    this.url = url;
  }

  int getStatusCode() {
    return statusCode;
  }

  String getUrl() {
    return url;
  }

  boolean isRetryable() {
    return statusCode == 429 || statusCode == 500 || statusCode == 502 || statusCode == 503;
  }

  /**
   * Whether a response is a spent rate limit rather than a refused credential.
   *
   * <p>GitHub answers an exhausted primary rate limit with 403, not 429, so without this the wait
   * is reported as "check token scopes" and the run fails immediately instead of backing off.
   */
  static boolean isRateLimited(java.net.http.HttpResponse<String> response) {
    return response
        .headers()
        .firstValue("X-RateLimit-Remaining")
        .map(remaining -> "0".equals(remaining.trim()))
        .orElse(false);
  }

  boolean isServerError() {
    return statusCode >= 500;
  }
}
