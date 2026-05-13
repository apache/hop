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

package org.apache.hop.lineage.model;

import java.util.Objects;
import lombok.Getter;

/** HTTP client or server observation. */
@Getter
public final class HttpLineagePayload implements LineagePayload {

  private final HttpDirection direction;
  private final String method;
  private final String url;
  private final Integer statusCode;
  private final Long requestBytes;
  private final Long responseBytes;
  private final Long durationMillis;
  private final boolean success;
  private final String message;

  public HttpLineagePayload(
      HttpDirection direction,
      String method,
      String url,
      Integer statusCode,
      Long requestBytes,
      Long responseBytes,
      Long durationMillis,
      boolean success,
      String message) {
    this.direction = Objects.requireNonNull(direction, "direction");
    this.method = method;
    this.url = url;
    this.statusCode = statusCode;
    this.requestBytes = requestBytes;
    this.responseBytes = responseBytes;
    this.durationMillis = durationMillis;
    this.success = success;
    this.message = message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HttpLineagePayload that = (HttpLineagePayload) o;
    return success == that.success
        && direction == that.direction
        && Objects.equals(method, that.method)
        && Objects.equals(url, that.url)
        && Objects.equals(statusCode, that.statusCode)
        && Objects.equals(requestBytes, that.requestBytes)
        && Objects.equals(responseBytes, that.responseBytes)
        && Objects.equals(durationMillis, that.durationMillis)
        && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        direction,
        method,
        url,
        statusCode,
        requestBytes,
        responseBytes,
        durationMillis,
        success,
        message);
  }
}
