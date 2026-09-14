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

import java.util.List;
import org.apache.hop.core.exception.HopException;

/**
 * Fetches notifications from one source - a release feed, a plugin catalog, a licence server - for
 * the Hop GUI to show.
 *
 * <p><b>For plugin developers:</b> annotate the implementation with {@link
 * NotificationProviderPlugin} and the plugin registry finds it the way it finds every other Hop
 * plugin. There is no registration call to make, and nothing to depend on beyond hop-core, so a
 * provider can live in a plugin that has no user interface of its own.
 *
 * <pre>
 * {@literal @}NotificationProviderPlugin(
 *     id = "my-plugin-notifications",
 *     name = "My Plugin",
 *     description = "Tells you when a new version of My Plugin is published")
 * public class MyPluginNotificationProvider implements INotificationProvider {
 *
 *   {@literal @}Override
 *   public List&lt;Notification&gt; fetchNotifications() throws HopException {
 *     Notification notification = new Notification();
 *     notification.setId("2.0");
 *     notification.setTitle("My Plugin 2.0 is available");
 *     notification.setMessage("You are running 1.9.");
 *     notification.setLink("https://example.com/plugin");
 *     notification.setTimestamp(new Date());
 *     return List.of(notification);
 *   }
 *
 *   // ... the remaining interface methods
 * }
 * </pre>
 *
 * <p><b>Identifiers.</b> The id of a notification only has to be unique among this provider's own
 * notifications: the service qualifies it with the source before storing it. It does have to be the
 * <em>same on every fetch</em> for the same thing, because that is how an already-seen notification
 * is recognised and how "read" is remembered. Derive it from something stable about the subject - a
 * version, a release tag, a feed entry id - never from a clock or a counter.
 *
 * <p><b>Failures.</b> Throw {@link HopException} when the source cannot be read. The service
 * catches it per provider, so one unreachable source does not stop the others, and reports it to
 * the user with a Retry button. Returning an empty list instead means the user is told nothing at
 * all.
 *
 * <p><b>Threading.</b> {@link #fetchNotifications()} is called on a polling thread, never on the
 * user interface thread, so it may block on network calls.
 */
public interface INotificationProvider {
  /**
   * @return Unique identifier for this provider
   */
  String getId();

  /**
   * @return Human-readable name for this provider
   */
  String getName();

  /**
   * @return Description of what this provider does
   */
  String getDescription();

  /**
   * Fetch notifications from the source
   *
   * @return List of notifications (may be empty, never null)
   * @throws HopException if there's an error fetching notifications
   */
  List<Notification> fetchNotifications() throws HopException;

  /**
   * @return Whether this provider is enabled
   */
  boolean isEnabled();

  /**
   * @param enabled Enable or disable this provider
   */
  void setEnabled(boolean enabled);

  /**
   * @return Poll interval in milliseconds
   */
  long getPollInterval();

  /**
   * @param interval Poll interval in milliseconds
   */
  void setPollInterval(long interval);

  /**
   * Initialize the provider
   *
   * @throws HopException if initialization fails
   */
  void initialize() throws HopException;

  /** Shutdown the provider and clean up resources */
  void shutdown();
}
