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

package org.apache.hop.ui.hopgui.notifications;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.ui.hopgui.HopGui;

/** Toolbar item for notifications (bell icon) */
@GuiPlugin
public class NotificationToolbarItem {

  public static final String ID_NOTIFICATION_BELL = "toolbar-00010-notifications";

  @GuiToolbarElement(
      root = HopGui.ID_NOTIFICATION_TOOLBAR,
      id = ID_NOTIFICATION_BELL,
      type = GuiToolbarElementType.BUTTON,
      toolTip = "i18n::NotificationToolbarItem.Tooltip",
      image = "ui/images/notification-bell.svg")
  public void showNotifications() {
    NotificationPanel panel = NotificationPanel.getInstance();
    if (panel != null) {
      panel.toggle();
    }
  }

  /** Test method to add a new notification - for debugging badge display */
  public static void addTestNotification() {
    org.apache.hop.core.notifications.Notification testNotif =
        new org.apache.hop.core.notifications.Notification(
            "test-manual-" + System.currentTimeMillis(),
            "Manual Test Notification",
            "This notification was manually added to test the badge display.",
            "Test",
            "manual-test",
            null,
            new java.util.Date(),
            org.apache.hop.core.notifications.NotificationPriority.INFO,
            org.apache.hop.core.notifications.NotificationCategory.OTHER);
    NotificationService.getInstance().addNotification(testNotif);
  }
}
