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
package org.apache.hop.ui.hopgui.notifications.providers;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Date;

/** Timestamp parsing shared by the notification providers. */
final class NotificationDates {

  private NotificationDates() {
    // Utility class
  }

  /**
   * Parse an ISO 8601 timestamp.
   *
   * <p>{@code SimpleDateFormat} with a quoted {@code 'Z'} silently reads a UTC instant as a local
   * one, putting every timestamp out by the machine's offset. {@code java.time} understands the
   * offset instead of skipping over it.
   *
   * @param value The timestamp, may be null
   * @return The instant, or null when the value is absent or unparseable
   */
  static Date parseIso(String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    String trimmed = value.trim();
    try {
      return Date.from(OffsetDateTime.parse(trimmed).toInstant());
    } catch (DateTimeParseException e) {
      // Not every feed writes the offset it is supposed to.
      try {
        return Date.from(LocalDateTime.parse(trimmed).toInstant(ZoneOffset.UTC));
      } catch (DateTimeParseException e2) {
        try {
          return Date.from(Instant.parse(trimmed));
        } catch (DateTimeParseException e3) {
          return null;
        }
      }
    }
  }
}
