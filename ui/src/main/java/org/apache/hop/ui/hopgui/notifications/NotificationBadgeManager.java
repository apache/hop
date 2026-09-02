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

import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/** Manages the badge indicator on the notification bell icon */
public class NotificationBadgeManager implements INotificationListener {
  private static NotificationBadgeManager instance;
  private ToolItem bellItem;
  private ToolBar toolbar;
  private PaintListener badgePaintListener;
  private int unreadCount = 0;

  private NotificationBadgeManager() {
    NotificationService.getInstance().addNotificationListener(this);
  }

  public static synchronized NotificationBadgeManager getInstance() {
    if (instance == null) {
      instance = new NotificationBadgeManager();
    }
    return instance;
  }

  /** Initialize the badge manager and find the bell icon */
  public void initialize() {
    // Retry initialization with delay to ensure toolbar is ready
    initializeWithRetry(0);
  }

  private void initializeWithRetry(int attempt) {
    Display.getCurrent()
        .asyncExec(
            () -> {
              try {
                // Skip badge on Hop Web (RAP): toolbar is Composite, not ToolBar; no ToolItems
                if (EnvironmentUtils.getInstance().isWeb()) {
                  return;
                }

                HopGui hopGui = HopGui.getInstance();
                if (hopGui != null && hopGui.getNotificationToolbar() != null) {
                  toolbar = hopGui.getNotificationToolbar();

                  // Try to find the bell item using the toolbar widgets
                  org.apache.hop.ui.core.gui.GuiToolbarWidgets toolbarWidgets =
                      hopGui.getNotificationToolbarWidgets();
                  if (toolbarWidgets != null) {
                    bellItem =
                        toolbarWidgets.findToolItem(
                            org.apache.hop.ui.hopgui.notifications.NotificationToolbarItem
                                .ID_NOTIFICATION_BELL);
                  }

                  // Fallback: find by iterating items (desktop ToolBar only)
                  if ((bellItem == null || bellItem.isDisposed()) && toolbar != null) {
                    ToolItem[] items = toolbar.getItems();
                    for (ToolItem item : items) {
                      if (item != null && !item.isDisposed()) {
                        bellItem = item;
                        break;
                      }
                    }
                  }

                  if (bellItem != null && !bellItem.isDisposed()) {
                    setupBadgePainting();
                    updateBadge();
                  } else if (attempt < 5) {
                    // Retry after a short delay
                    Display.getCurrent().timerExec(200, () -> initializeWithRetry(attempt + 1));
                  }
                } else if (attempt < 5) {
                  // Retry after a short delay
                  Display.getCurrent().timerExec(200, () -> initializeWithRetry(attempt + 1));
                }
              } catch (Exception e) {
                // Retry on error
                if (attempt < 5) {
                  Display.getCurrent().timerExec(200, () -> initializeWithRetry(attempt + 1));
                }
              }
            });
  }

  /** Setup painting listener for the badge */
  private void setupBadgePainting() {
    if (toolbar == null || toolbar.isDisposed() || bellItem == null || bellItem.isDisposed()) {
      return;
    }

    // Remove existing listener if any
    if (badgePaintListener != null) {
      try {
        toolbar.removePaintListener(badgePaintListener);
      } catch (Exception e) {
        // Ignore
      }
    }

    badgePaintListener =
        new PaintListener() {
          @Override
          public void paintControl(PaintEvent e) {
            // Always check unread count fresh in case it changed
            int currentUnreadCount = NotificationService.getInstance().getUnreadCount();

            // If count is 0, we don't draw anything (badge should be hidden)
            // The erase=true in redraw() should have cleared the area
            if (currentUnreadCount <= 0) {
              return; // Don't draw badge when count is 0
            }

            if (bellItem != null && !bellItem.isDisposed()) {
              try {
                org.eclipse.swt.graphics.Rectangle itemBounds = bellItem.getBounds();

                // Draw red circle badge in top-right corner of the icon
                int badgeSize = 12; // Made slightly larger for visibility
                int badgeX = itemBounds.x + itemBounds.width - badgeSize - 1;
                int badgeY = itemBounds.y + 1;

                GuiResource guiResource = GuiResource.getInstance();
                Color redColor = guiResource.getColorRed();

                e.gc.setBackground(redColor);
                e.gc.setAntialias(SWT.ON);
                e.gc.fillOval(badgeX, badgeY, badgeSize, badgeSize);

                // Draw white border for better visibility
                e.gc.setForeground(guiResource.getColorWhite());
                e.gc.setLineWidth(2);
                e.gc.drawOval(badgeX, badgeY, badgeSize - 1, badgeSize - 1);
              } catch (Exception ex) {
                // Ignore paint errors
              }
            }
          }
        };

    toolbar.addPaintListener(badgePaintListener);
  }

  /** Update the badge display */
  private void updateBadge() {
    if (toolbar == null || toolbar.isDisposed()) {
      return;
    }

    // Update unread count - always get fresh count
    int newUnreadCount = NotificationService.getInstance().getUnreadCount();
    unreadCount = newUnreadCount;

    // Ensure paint listener is set up
    if (badgePaintListener == null && bellItem != null && !bellItem.isDisposed()) {
      setupBadgePainting();
    }

    if (toolbar != null && !toolbar.isDisposed() && bellItem != null && !bellItem.isDisposed()) {
      try {
        // Get the bell item bounds for precise redraw
        org.eclipse.swt.graphics.Rectangle bounds = bellItem.getBounds();

        // Force redraw of the bell item area specifically
        // Use erase=true to clear the old badge when count reaches 0
        toolbar.redraw(bounds.x, bounds.y, bounds.width, bounds.height, true);

        // Also trigger a full toolbar update to ensure everything is in sync
        toolbar.update();
      } catch (Exception e) {
        // If precise redraw fails, fall back to full toolbar redraw
        try {
          toolbar.redraw();
          toolbar.update();
        } catch (Exception e2) {
          // Ignore
        }
      }
    }
  }

  @Override
  public void notificationsChanged() {
    // Update immediately on the UI thread
    Display display = Display.getCurrent();
    if (display == null) {
      display = Display.getDefault();
    }
    display.asyncExec(
        () -> {
          try {
            // Get fresh unread count
            int currentCount = NotificationService.getInstance().getUnreadCount();
            unreadCount = currentCount;

            // If we don't have the bell item yet, try to initialize
            if (bellItem == null || bellItem.isDisposed()) {
              initializeWithRetry(0);
            } else {
              // Update badge immediately
              updateBadge();
            }
          } catch (Exception e) {
            // Ignore errors but try to reinitialize if needed
            if (bellItem == null || bellItem.isDisposed()) {
              initializeWithRetry(0);
            } else {
              // Still try to update even if there was an error
              try {
                updateBadge();
              } catch (Exception e2) {
                // Ignore
              }
            }
          }
        });
  }

  /** Dispose the badge manager */
  public void dispose() {
    if (badgePaintListener != null && toolbar != null && !toolbar.isDisposed()) {
      try {
        toolbar.removePaintListener(badgePaintListener);
      } catch (Exception e) {
        // Ignore
      }
    }
    NotificationService.getInstance().removeNotificationListener(this);
    instance = null;
  }
}
