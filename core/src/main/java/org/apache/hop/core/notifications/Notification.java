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

package org.apache.hop.core.notifications;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/** Represents a single notification */
@Getter
@Setter
public class Notification {
  private String id; // Unique identifier
  private String title; // Notification title
  private String message; // Notification body/message
  private String source; // "rss", "github", "plugin", etc.
  private String sourceId; // Provider-specific ID
  private String link; // URL to open when clicked
  private Date timestamp; // When notification was created
  private Date receivedAt; // When we received it
  private boolean read; // Read/unread state
  private NotificationPriority priority; // INFO, WARNING, ERROR
  private NotificationCategory category; // RELEASE, ANNOUNCEMENT, PLUGIN, etc.
  private Map<String, Object> metadata; // Provider-specific data
  private String version; // For version-related notifications

  public Notification() {
    this.metadata = new HashMap<>();
    this.receivedAt = new Date();
    this.read = false;
    this.priority = NotificationPriority.INFO;
    this.category = NotificationCategory.OTHER;
  }

  public Notification(
      String id,
      String title,
      String message,
      String source,
      String sourceId,
      String link,
      Date timestamp,
      NotificationPriority priority,
      NotificationCategory category) {
    this();
    this.id = id;
    this.title = title;
    this.message = message;
    this.source = source;
    this.sourceId = sourceId;
    this.link = link;
    this.timestamp = timestamp != null ? timestamp : new Date();
    this.priority = priority != null ? priority : NotificationPriority.INFO;
    this.category = category != null ? category : NotificationCategory.OTHER;
  }
}
