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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * Parses the timestamp formats the supported providers emit into {@link Date} values.
 *
 * <p>The providers are not consistent: GitHub and Gitea/Forgejo return {@code 2026-05-01T12:00:00Z}
 * or an explicit offset, GitLab adds milliseconds ({@code 2026-05-01T12:00:00.000Z}), and Bitbucket
 * uses microsecond precision with a {@code +00:00} style offset. All of these are valid ISO-8601,
 * so a small cascade covers every case without provider-specific parsing.
 */
final class GitTimestamps {

  private GitTimestamps() {}

  /**
   * Converts a provider timestamp to a {@link Date}, or returns {@code null} when the value is
   * absent or cannot be parsed. Unparseable values are never an error: the original text is always
   * still available in {@code raw_json}.
   */
  static Date toDate(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    String trimmed = value.trim();
    try {
      return Date.from(Instant.parse(trimmed));
    } catch (DateTimeParseException ignored) {
      // not a plain instant; try the offset-bearing forms below
    }
    try {
      return Date.from(OffsetDateTime.parse(trimmed).toInstant());
    } catch (DateTimeParseException ignored) {
      // not an offset date-time; try a bare local date-time below
    }
    try {
      return Date.from(LocalDateTime.parse(trimmed).toInstant(ZoneOffset.UTC));
    } catch (DateTimeParseException ignored) {
      return null;
    }
  }
}
