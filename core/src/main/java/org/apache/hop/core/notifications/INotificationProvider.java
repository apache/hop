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
 * Interface for notification providers. Notification providers fetch notifications from various
 * sources (GitHub releases, RSS feeds, custom plugins, etc.) and make them available to the Hop GUI
 * notification system.
 *
 * <p><b>For Plugin Developers:</b>
 *
 * <p>Custom plugins can register their own notification providers using the {@link
 * org.apache.hop.ui.hopgui.notifications.NotificationPluginHelper} helper class. This is the
 * recommended approach as it automatically:
 *
 * <ul>
 *   <li>Checks if a notification source already exists for your plugin
 *   <li>Creates and configures a notification source if it doesn't exist
 *   <li>Registers your notification provider
 * </ul>
 *
 * <p><b>Example Implementation:</b>
 *
 * <pre>
 * public class MyPluginNotificationProvider implements INotificationProvider {
 *   private String id = "my-plugin-notifications";
 *   private boolean enabled = true;
 *   private long pollInterval = 3600000; // 1 hour
 *
 *   {@literal @}Override
 *   public List&lt;Notification&gt; fetchNotifications() throws HopException {
 *     List&lt;Notification&gt; notifications = new ArrayList&lt;&gt;();
 *     // Fetch notifications from your plugin's source
 *     Notification notif = new Notification();
 *     notif.setId("my-notif-1");
 *     notif.setTitle("Plugin Update Available");
 *     notif.setMessage("Version 2.0 is now available!");
 *     notif.setSource("my-plugin");
 *     notif.setSourceId("my-plugin-notifications"); // Must match provider.getId()
 *     notif.setLink("https://example.com/plugin/download");
 *     notif.setTimestamp(new Date());
 *     notifications.add(notif);
 *     return notifications;
 *   }
 *
 *   // ... implement other interface methods
 * }
 * </pre>
 *
 * <p>Then register it during plugin initialization:
 *
 * <pre>
 * {@literal @}GuiPlugin(description = "My Plugin")
 * public class MyPluginGuiPlugin {
 *   public MyPluginGuiPlugin() {
 *     INotificationProvider provider = new MyPluginNotificationProvider();
 *
 *     // Auto-register notification source and provider
 *     NotificationPluginHelper.registerPluginNotifications(
 *         "my-plugin-notifications",        // Plugin ID (must match provider.getId())
 *         "My Plugin Notifications",        // Display name
 *         provider,                         // Your provider instance
 *         60,                               // Poll interval in minutes (optional)
 *         "#FF5733"                         // Color hex code (optional)
 *     );
 *   }
 * }
 * </pre>
 *
 * <p><b>Performance Impact:</b> The registration process is very fast (&lt; 2ms per plugin) and has
 * negligible impact on startup time. The check for existing sources is O(n) where n is typically
 * &lt; 10, and all operations use in-memory data structures.
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
