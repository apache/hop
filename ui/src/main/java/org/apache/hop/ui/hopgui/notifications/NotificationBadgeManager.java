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

import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ISingletonProvider;
import org.apache.hop.ui.hopgui.ImplementationLoader;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/** Manages the badge indicator on the notification bell icon */
public class NotificationBadgeManager implements INotificationListener {

  private static final String BELL_ICON = "ui/images/notification-bell.svg";
  private static final String BELL_UNREAD_ICON = "ui/images/notification-bell-unread.svg";
  private static NotificationBadgeManager fallback;

  private static final ISingletonProvider PROVIDER = loadProvider();

  private static ISingletonProvider loadProvider() {
    try {
      return (ISingletonProvider) ImplementationLoader.newInstance(NotificationBadgeManager.class);
    } catch (Throwable e) {
      // hop-ui unit tests have no rcp/rap *Impl on the classpath. Anywhere else this is a
      // misconfiguration worth shouting about: one instance would then be shared by every Hop Web
      // session, which is the very thing the per-session provider exists to prevent.
      LogChannel.GENERAL.logBasic(
          "No NotificationBadgeManagerImpl found; falling back to a single instance for this process. "
              + "In Hop Web that means every session shares one.");
      return () -> {
        synchronized (NotificationBadgeManager.class) {
          if (fallback == null) {
            fallback = new NotificationBadgeManager();
          }
          return fallback;
        }
      };
    }
  }

  /**
   * The display this manager belongs to, captured while initialising on the user interface thread.
   *
   * <p>A poll runs on a background thread, where {@code Display.getCurrent()} is null and, under
   * RAP, {@code HopGui.peekInstance()} is null as well because there is no session context. In Hop
   * Web there is also no bell ToolItem to read a display from. Remembering it while we are on the
   * user interface thread is the only reliable way back to it.
   */
  private Display display;

  private ToolItem bellItem;
  private ToolBar toolbar;
  private PaintListener badgePaintListener;
  private int unreadCount = 0;

  /** Use {@link #getInstance()}. Public so RWT can create one per user session in Hop Web. */
  public NotificationBadgeManager() {
    NotificationService.getInstance().addNotificationListener(this);
  }

  /**
   * @return The badge manager of this process, or of this user's session in Hop Web
   */
  public static NotificationBadgeManager getInstance() {
    return (NotificationBadgeManager) PROVIDER.getInstanceInternal();
  }

  /**
   * Whether a paint listener can be attached to the toolbar carrying the bell.
   *
   * @return false under RAP, where only a Canvas can be painted on
   */
  private boolean canPaintOnToolbar() {
    return !EnvironmentUtils.getInstance().isWeb();
  }

  /**
   * Show whether anything is unread by swapping the bell icon.
   *
   * <p>The desktop also draws a counted badge over the toolbar, but RAP offers a paint listener on
   * a Canvas only, so in Hop Web the icon itself has to carry the dot. {@code
   * GuiToolbarWidgets.setToolbarItemImage} already knows how to change an icon on either platform:
   * a ToolItem image on the desktop, the SVG inside the label under RAP.
   */
  private void updateBellIcon() {
    try {
      HopGui hopGui = HopGui.peekInstance();
      if (hopGui == null || hopGui.getNotificationToolbarWidgets() == null) {
        return;
      }
      // Load the icon before asking the toolbar for it. SvgCache.findSvg is a plain cache lookup,
      // and Hop Web builds a toolbar icon out of that cache, so an SVG nobody has drawn yet
      // resolves to nothing and the swap is skipped without a word. The bell we start with is
      // cached while the toolbar is built; the one carrying the dot never is.
      cacheIcon(unreadCount > 0 ? BELL_UNREAD_ICON : BELL_ICON);
      hopGui
          .getNotificationToolbarWidgets()
          .setToolbarItemImage(
              NotificationToolbarItem.ID_NOTIFICATION_BELL,
              unreadCount > 0 ? BELL_UNREAD_ICON : BELL_ICON);
    } catch (Exception e) {
      LogChannel.UI.logDetailed("Unable to update the notification bell icon: " + e.getMessage());
    }
  }

  /**
   * Make sure an icon is in the SVG cache, so the toolbar is able to render it.
   *
   * @param icon The classpath location of the icon
   */
  private void cacheIcon(String icon) {
    try {
      int size = (int) (ConstUi.SMALL_ICON_SIZE * PropsUi.getNativeZoomFactor());
      GuiResource.getInstance().getImage(icon, getClass().getClassLoader(), size, size);
    } catch (Exception e) {
      LogChannel.UI.logDetailed("Unable to load notification icon " + icon + ": " + e.getMessage());
    }
  }

  /**
   * The display the bell lives on, or null when the toolbar has not been found yet.
   *
   * @return The display owning the bell widgets
   */
  private Display displayOfBell() {
    if (display != null && !display.isDisposed()) {
      return display;
    }
    try {
      if (toolbar != null && !toolbar.isDisposed()) {
        return toolbar.getDisplay();
      }
      if (bellItem != null && !bellItem.isDisposed()) {
        return bellItem.getDisplay();
      }
      HopGui hopGui = HopGui.peekInstance();
      if (hopGui != null && hopGui.getShell() != null && !hopGui.getShell().isDisposed()) {
        return hopGui.getShell().getDisplay();
      }
    } catch (SWTException e) {
      // Disposed between the check and the call; there is nothing to update.
    }
    return null;
  }

  /** Initialize the badge manager and find the bell icon */
  public void initialize() {
    // Retry initialization with delay to ensure toolbar is ready
    initializeWithRetry(0);
  }

  private void initializeWithRetry(int attempt) {
    // Runs on the user interface thread: remember the display before anything can return early.
    if (display == null) {
      display = Display.getCurrent();
    }
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
        if (canPaintOnToolbar()) {
          toolbar.removePaintListener(badgePaintListener);
        }
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

    if (!canPaintOnToolbar()) {
      // Hop Web: RAP offers a paint listener on a Canvas only, so nothing can be drawn over the
      // bell. Swapping the bell icon for one that carries a dot says the same thing.
      return;
    }
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
    // Providers are polled on a background thread, where Display.getCurrent() is null. Falling
    // back to Display.getDefault() works on the desktop but returns null under RAP, where a
    // display belongs to a session and there is no default one: the badge then never updated in
    // Hop Web. The toolbar carrying the bell knows which display is the right one.
    Display display = Display.getCurrent();
    if (display == null) {
      display = displayOfBell();
    }
    if (display == null) {
      // Nothing has been drawn yet; the badge is painted when the bell is found.
      return;
    }
    display.asyncExec(
        () -> {
          try {
            // Get fresh unread count
            int currentCount = NotificationService.getInstance().getUnreadCount();
            unreadCount = currentCount;

            // If we don't have the bell item yet, try to initialize
            // The icon carries the unread state on both platforms. The painted badge, with its
            // count, is desktop only: there is no bell ToolItem in Hop Web to paint over.
            updateBellIcon();
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
        if (canPaintOnToolbar()) {
          toolbar.removePaintListener(badgePaintListener);
        }
      } catch (Exception e) {
        // Ignore
      }
    }
    NotificationService.getInstance().removeNotificationListener(this);
  }
}
