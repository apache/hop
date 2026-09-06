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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;

/** Test provider for manual testing of the notification system */
public class TestNotificationProvider implements INotificationProvider {
  private boolean enabled = true;
  private long pollInterval = 60000; // 1 minute

  @Override
  public String getId() {
    return "test-provider";
  }

  @Override
  public String getName() {
    return "Test Notification Provider";
  }

  @Override
  public String getDescription() {
    return "Provides test notifications for manual testing";
  }

  @Override
  public List<Notification> fetchNotifications() throws HopException {
    List<Notification> notifications = new ArrayList<>();
    long now = System.currentTimeMillis();

    // Notification 0: Test notification (just now - always unread for testing)
    Notification testNotification =
        new Notification(
            "test-notification-new",
            "New Test Notification",
            "This is a test notification to verify the badge appears. Click 'Mark all read' to remove the badge.",
            "Test System",
            "test-provider",
            "https://hop.apache.org",
            new Date(now), // Just now
            NotificationPriority.INFO,
            NotificationCategory.OTHER);
    notifications.add(testNotification);

    // Notification 1: New Release (5 minutes ago)
    Notification releaseNotification =
        new Notification(
            "test-notification-1",
            "Apache Hop 2.7.0 Released",
            "A new version of Apache Hop is now available with improved performance, bug fixes, and new features including enhanced pipeline execution monitoring.",
            "Apache Hop",
            "test-provider",
            "https://hop.apache.org/download",
            new Date(now - 5 * 60000), // 5 minutes ago
            NotificationPriority.INFO,
            NotificationCategory.RELEASE);
    releaseNotification.setVersion("2.7.0");
    notifications.add(releaseNotification);

    // Notification 2: Plugin Update Available (2 hours ago)
    Notification pluginNotification =
        new Notification(
            "test-notification-2",
            "Plugin Update Available",
            "A newer version of the 'Database Connectors' plugin is available. Update to version 1.3.2 for improved database connectivity and performance enhancements.",
            "Plugin Manager",
            "test-provider",
            "https://hop.apache.org/plugins",
            new Date(now - 2 * 3600000), // 2 hours ago
            NotificationPriority.INFO,
            NotificationCategory.PLUGIN);
    pluginNotification.setVersion("1.3.2");
    notifications.add(pluginNotification);

    // Notification 3: Security Announcement (1 day ago)
    Notification securityNotification =
        new Notification(
            "test-notification-3",
            "Security Update Recommended",
            "A security vulnerability has been identified in Apache Hop versions prior to 2.6.5. Please update to the latest version to ensure your installation is secure.",
            "Apache Hop Security Team",
            "test-provider",
            "https://hop.apache.org/security",
            new Date(now - 86400000), // 1 day ago
            NotificationPriority.WARNING,
            NotificationCategory.SECURITY);
    notifications.add(securityNotification);

    // Notification 4: General Announcement (3 days ago)
    Notification announcementNotification =
        new Notification(
            "test-notification-4",
            "Community Webinar: Advanced Pipeline Design",
            "Join us for a free webinar on advanced pipeline design patterns and best practices. Learn from Apache Hop experts and get your questions answered.",
            "Apache Hop Community",
            "test-provider",
            "https://hop.apache.org/events",
            new Date(now - 3 * 86400000), // 3 days ago
            NotificationPriority.INFO,
            NotificationCategory.ANNOUNCEMENT);
    notifications.add(announcementNotification);

    return notifications;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  public long getPollInterval() {
    return pollInterval;
  }

  @Override
  public void setPollInterval(long interval) {
    this.pollInterval = interval;
  }

  @Override
  public void initialize() throws HopException {
    // Nothing to initialize for test provider
  }

  @Override
  public void shutdown() {
    // Nothing to clean up for test provider
  }
}
