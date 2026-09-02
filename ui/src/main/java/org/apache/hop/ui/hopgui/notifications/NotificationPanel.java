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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CLabel;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Dropdown panel for displaying notifications */
public class NotificationPanel implements INotificationListener {
  private static final Class<?> PKG = NotificationPanel.class;
  private static NotificationPanel instance;

  private Shell shell;
  private Shell parentShell;
  private ScrolledComposite scrolledComposite;
  private Composite contentComposite;
  private boolean isVisible = false;

  private NotificationPanel() {
    this.parentShell = HopGui.getInstance().getShell();
    NotificationService.getInstance().addNotificationListener(this);
  }

  public static synchronized NotificationPanel getInstance() {
    if (instance == null) {
      instance = new NotificationPanel();
    }
    return instance;
  }

  /** Toggle the panel visibility */
  public void toggle() {
    if (isVisible) {
      hide();
    } else {
      show();
    }
  }

  /** Show the notification panel */
  public void show() {
    if (shell != null && !shell.isDisposed()) {
      // Panel already exists, refresh notifications and show
      updateNotifications();
      shell.setVisible(true);
      shell.setFocus();
      return;
    }

    createPanel();
    updateNotifications();
    positionPanel();
    shell.setVisible(true);
    isVisible = true;
  }

  /** Hide the notification panel */
  public void hide() {
    if (shell != null && !shell.isDisposed()) {
      shell.setVisible(false);
    }
    isVisible = false;
  }

  /** Create the panel UI */
  private void createPanel() {
    // Use DIALOG_TRIM instead of ON_TOP to keep it attached to parent
    // Remove ON_TOP so it doesn't stay on top when switching applications
    shell = new Shell(parentShell, SWT.DIALOG_TRIM | SWT.RESIZE);
    shell.setLayout(new FormLayout());
    PropsUi.setLook(shell);

    // Header
    Composite header = new Composite(shell, SWT.NONE);
    header.setLayout(new FormLayout());
    PropsUi.setLook(header);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(0, 0);
    fdHeader.right = new FormAttachment(100, 0);
    fdHeader.top = new FormAttachment(0, 0);
    header.setLayoutData(fdHeader);

    // Settings button
    Button settingsButton = new Button(header, SWT.PUSH);
    settingsButton.setText(BaseMessages.getString(PKG, "NotificationPanel.Settings"));
    settingsButton.setToolTipText(
        BaseMessages.getString(PKG, "NotificationPanel.Settings.Tooltip"));
    PropsUi.setLook(settingsButton);
    FormData fdSettings = new FormData();
    fdSettings.right = new FormAttachment(100, -10);
    fdSettings.top = new FormAttachment(0, 5);
    fdSettings.bottom = new FormAttachment(100, -5);
    settingsButton.setLayoutData(fdSettings);
    settingsButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective
                configPerspective = HopGui.getConfigurationPerspective();
            if (configPerspective != null) {
              HopGui.getInstance().setActivePerspective(configPerspective);

              // Defer tab/tree selection until perspective is fully activated
              Display.getCurrent()
                  .asyncExec(
                      () -> {
                        ConfigurationPerspective perspective = HopGui.getConfigurationPerspective();
                        if (perspective != null) {
                          perspective.showNotificationsTab();
                        }
                      });
            }
          }
        });

    Button clearAll = new Button(header, SWT.PUSH);
    clearAll.setText(BaseMessages.getString(PKG, "NotificationPanel.ClearAll"));
    clearAll.setToolTipText(BaseMessages.getString(PKG, "NotificationPanel.ClearAll.Tooltip"));
    PropsUi.setLook(clearAll);
    FormData fdClearAll = new FormData();
    fdClearAll.right = new FormAttachment(settingsButton, -10);
    fdClearAll.top = new FormAttachment(0, 5);
    fdClearAll.bottom = new FormAttachment(100, -5);
    clearAll.setLayoutData(fdClearAll);
    clearAll.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            NotificationService.getInstance().clearAll();
            updateNotifications();
          }
        });

    Button markAllRead = new Button(header, SWT.PUSH);
    markAllRead.setText(BaseMessages.getString(PKG, "NotificationPanel.MarkAllRead"));
    PropsUi.setLook(markAllRead);
    FormData fdMarkAll = new FormData();
    fdMarkAll.right = new FormAttachment(clearAll, -10);
    fdMarkAll.top = new FormAttachment(0, 5);
    fdMarkAll.bottom = new FormAttachment(100, -5);
    markAllRead.setLayoutData(fdMarkAll);
    markAllRead.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            NotificationService.getInstance().markAllAsRead();
            updateNotifications();
          }
        });

    // Scrolled content area
    scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.BORDER);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolled = new FormData();
    fdScrolled.left = new FormAttachment(0, 0);
    fdScrolled.right = new FormAttachment(100, 0);
    fdScrolled.top = new FormAttachment(header, 0);
    fdScrolled.bottom = new FormAttachment(100, -40);
    scrolledComposite.setLayoutData(fdScrolled);

    contentComposite = new Composite(scrolledComposite, SWT.NONE);
    contentComposite.setLayout(new FormLayout());
    PropsUi.setLook(contentComposite);
    scrolledComposite.setContent(contentComposite);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);

    // Footer
    Composite footer = new Composite(shell, SWT.NONE);
    footer.setLayout(new FormLayout());
    PropsUi.setLook(footer);
    FormData fdFooter = new FormData();
    fdFooter.left = new FormAttachment(0, 0);
    fdFooter.right = new FormAttachment(100, 0);
    fdFooter.bottom = new FormAttachment(100, 0);
    footer.setLayoutData(fdFooter);

    Button closeButton = new Button(footer, SWT.PUSH);
    closeButton.setText(BaseMessages.getString(PKG, "NotificationPanel.Close"));
    PropsUi.setLook(closeButton);
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment(100, -10);
    fdClose.top = new FormAttachment(0, 5);
    fdClose.bottom = new FormAttachment(100, -5);
    closeButton.setLayoutData(fdClose);
    closeButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            hide();
          }
        });

    // Close when clicking outside
    shell.addListener(
        SWT.Deactivate,
        e -> {
          if (!shell.isDisposed()) {
            Display.getCurrent()
                .asyncExec(
                    () -> {
                      if (!shell.isDisposed() && !shell.isFocusControl()) {
                        hide();
                      }
                    });
          }
        });

    // Listen to parent shell resize events to reposition panel
    if (parentShell != null && !parentShell.isDisposed()) {
      parentShell.addListener(
          SWT.Resize,
          e -> {
            if (shell != null && !shell.isDisposed() && isVisible) {
              Display.getCurrent()
                  .asyncExec(
                      () -> {
                        if (shell != null && !shell.isDisposed() && isVisible) {
                          positionPanel();
                        }
                      });
            }
          });
    }

    shell.setSize(400, 500);

    // Add listener to shell resize to ensure titles truncate properly
    shell.addListener(
        SWT.Resize,
        e -> {
          if (contentComposite != null && !contentComposite.isDisposed()) {
            // Force layout update to ensure titles truncate correctly
            contentComposite.layout(true, true);
          }
        });
  }

  /** Update the notifications display */
  private void updateNotifications() {
    if (contentComposite == null || contentComposite.isDisposed()) {
      return;
    }

    // Clear existing notifications
    for (Control control : contentComposite.getChildren()) {
      control.dispose();
    }

    // Get configuration options
    org.apache.hop.core.config.HopConfig hopConfig =
        org.apache.hop.core.config.HopConfig.getInstance();
    boolean showReadNotifications =
        org.apache.hop.core.config.HopConfig.readOptionString(
                "notification.showReadNotifications", "true")
            .equalsIgnoreCase("true");
    String daysToGoBackStr =
        org.apache.hop.core.config.HopConfig.readOptionString(
            "notification.global.daysToGoBack", "30");
    int daysToGoBack = 0;
    try {
      daysToGoBack = Integer.parseInt(daysToGoBackStr);
    } catch (NumberFormatException e) {
      daysToGoBack = 30; // Default to 30 days
    }

    // Get provider errors and notifications
    List<org.apache.hop.ui.hopgui.notifications.ProviderErrorInfo> providerErrors =
        NotificationService.getInstance().getProviderErrors();
    List<Notification> notifications =
        NotificationService.getInstance().getNotifications(!showReadNotifications, daysToGoBack);

    Control lastControl = null;

    // Provider error banner
    if (!providerErrors.isEmpty()) {
      Composite errorBanner = createProviderErrorBanner(providerErrors, lastControl);
      lastControl = errorBanner;
    }

    if (notifications.isEmpty() && lastControl == null) {
      Label emptyLabel = new Label(contentComposite, SWT.CENTER | SWT.WRAP);
      emptyLabel.setText(BaseMessages.getString(PKG, "NotificationPanel.NoNotifications"));
      PropsUi.setLook(emptyLabel);
      FormData fdEmpty = new FormData();
      fdEmpty.left = new FormAttachment(0, 10);
      fdEmpty.right = new FormAttachment(100, -10);
      fdEmpty.top = new FormAttachment(0, 20);
      emptyLabel.setLayoutData(fdEmpty);
    } else if (!notifications.isEmpty()) {
      for (Notification notification : notifications) {
        try {
          Composite notifComposite = createNotificationItem(notification, lastControl);
          lastControl = notifComposite;
        } catch (Exception e) {
          // Log error but continue with other notifications
          org.apache.hop.core.logging.LogChannel.UI.logError(
              "Error creating notification item: " + notification.getTitle(), e);
        }
      }
    } else if (lastControl != null && notifications.isEmpty()) {
      // Errors only, no notifications
      Label emptyLabel = new Label(contentComposite, SWT.CENTER | SWT.WRAP);
      emptyLabel.setText(BaseMessages.getString(PKG, "NotificationPanel.NoNotifications"));
      PropsUi.setLook(emptyLabel);
      FormData fdEmpty = new FormData();
      fdEmpty.left = new FormAttachment(0, 10);
      fdEmpty.right = new FormAttachment(100, -10);
      fdEmpty.top = new FormAttachment(lastControl, 10);
      emptyLabel.setLayoutData(fdEmpty);
    }

    // Force layout of content composite and scrolled composite
    if (contentComposite != null && !contentComposite.isDisposed()) {
      // Get the scrolled composite width first to ensure proper sizing
      int availableWidth = SWT.DEFAULT;
      if (scrolledComposite != null && !scrolledComposite.isDisposed()) {
        org.eclipse.swt.graphics.Rectangle scrolledBounds = scrolledComposite.getBounds();
        if (scrolledBounds.width > 0) {
          availableWidth = scrolledBounds.width - 20; // Account for margins
        }
      }

      // Layout scrolled composite first to get its actual width
      if (scrolledComposite != null && !scrolledComposite.isDisposed()) {
        scrolledComposite.layout(true, false);
        org.eclipse.swt.graphics.Rectangle scrolledBounds = scrolledComposite.getBounds();
        if (scrolledBounds.width > 0) {
          availableWidth = scrolledBounds.width - 20; // Account for margins
        } else {
          // Fallback: use shell width if scrolled composite not sized yet
          if (shell != null && !shell.isDisposed()) {
            availableWidth = shell.getSize().x > 0 ? shell.getSize().x - 40 : 380;
          } else {
            availableWidth = 380; // Default width
          }
        }
      }

      // Layout content composite with proper width constraint
      // This is critical for SWT.WRAP labels to calculate their height
      contentComposite.layout(true, true);

      // Compute size with width constraint for proper wrapping
      org.eclipse.swt.graphics.Point contentSize =
          contentComposite.computeSize(availableWidth, SWT.DEFAULT);

      if (scrolledComposite != null && !scrolledComposite.isDisposed()) {
        scrolledComposite.setMinSize(contentSize);
        scrolledComposite.layout(true, true);
      }
    }
  }

  /** Create the provider error banner with Retry button */
  private Composite createProviderErrorBanner(
      List<org.apache.hop.ui.hopgui.notifications.ProviderErrorInfo> errors, Control above) {
    Composite banner = new Composite(contentComposite, SWT.BORDER);
    banner.setLayout(new FormLayout());
    PropsUi.setLook(banner);
    banner.setBackground(GuiResource.getInstance().getColor(255, 248, 220)); // Light yellow / wheat

    FormData fdBanner = new FormData();
    fdBanner.left = new FormAttachment(0, 0);
    fdBanner.right = new FormAttachment(100, 0);
    fdBanner.top = above != null ? new FormAttachment(above, 10) : new FormAttachment(0, 10);
    banner.setLayoutData(fdBanner);

    Label headerLabel = new Label(banner, SWT.WRAP);
    headerLabel.setText(BaseMessages.getString(PKG, "NotificationPanel.ProviderErrors"));
    headerLabel.setBackground(banner.getBackground());
    PropsUi.setLook(headerLabel);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(0, 10);
    fdHeader.right = new FormAttachment(100, -80);
    fdHeader.top = new FormAttachment(0, 10);
    headerLabel.setLayoutData(fdHeader);

    Button retryButton = new Button(banner, SWT.PUSH);
    retryButton.setText(BaseMessages.getString(PKG, "NotificationPanel.Retry"));
    retryButton.setToolTipText(BaseMessages.getString(PKG, "NotificationPanel.Retry.Tooltip"));
    PropsUi.setLook(retryButton);
    FormData fdRetry = new FormData();
    fdRetry.right = new FormAttachment(100, -10);
    fdRetry.top = new FormAttachment(0, 5);
    retryButton.setLayoutData(fdRetry);
    retryButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            NotificationService.getInstance().retryNow();
            updateNotifications();
          }
        });

    Control lastLine = headerLabel;
    for (org.apache.hop.ui.hopgui.notifications.ProviderErrorInfo err : errors) {
      String text =
          BaseMessages.getString(
              PKG, "NotificationPanel.ProviderErrorItem", err.getProviderName(), err.getMessage());
      Label line = new Label(banner, SWT.WRAP);
      line.setText(text);
      line.setBackground(banner.getBackground());
      PropsUi.setLook(line);
      FormData fdLine = new FormData();
      fdLine.left = new FormAttachment(0, 10);
      fdLine.right = new FormAttachment(100, -10);
      fdLine.top = new FormAttachment(lastLine, 5);
      line.setLayoutData(fdLine);
      lastLine = line;
    }

    return banner;
  }

  /** Create a notification item UI */
  private Composite createNotificationItem(Notification notification, Control above) {
    Composite composite = new Composite(contentComposite, SWT.BORDER);
    composite.setLayout(new FormLayout());
    PropsUi.setLook(composite);

    GuiResource guiResource = GuiResource.getInstance();

    // Set background color based on read state and priority
    // Note: We'll update this dynamically when notification is marked as read
    updateNotificationBackground(composite, notification, guiResource);

    // Set FormData for positioning in parent
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    if (above != null) {
      fdComposite.top = new FormAttachment(above, 5);
    } else {
      fdComposite.top = new FormAttachment(0, 5);
    }
    // Don't set bottom - composite will size to its children
    composite.setLayoutData(fdComposite);

    // Priority indicator (colored bar on the left)
    Composite priorityBar = new Composite(composite, SWT.NONE);
    priorityBar.setLayout(null);
    priorityBar.setData("type", "priorityBar"); // Mark for later updates
    FormData fdPriorityBar = new FormData();
    fdPriorityBar.left = new FormAttachment(0, 0);
    fdPriorityBar.top = new FormAttachment(0, 0);
    fdPriorityBar.bottom = new FormAttachment(100, 0);
    fdPriorityBar.width = 4;
    priorityBar.setLayoutData(fdPriorityBar);

    // Set initial priority bar color (will be updated when read state changes)
    updatePriorityBar(priorityBar, notification, guiResource);

    // Source color indicator (small colored square) - positioned on the left, after priority bar
    Composite sourceIndicator = new Composite(composite, SWT.NONE);
    sourceIndicator.setLayout(null);
    sourceIndicator.setData("type", "sourceIndicator"); // Mark to exclude from click handling
    PropsUi.setLook(sourceIndicator);
    // Get color for this source from configuration
    org.eclipse.swt.graphics.Color sourceColor = getSourceColor(notification, guiResource);
    sourceIndicator.setBackground(sourceColor);
    FormData fdSourceIndicator = new FormData();
    // Position after priority bar, 8px gap
    fdSourceIndicator.left = new FormAttachment(priorityBar, 8);
    fdSourceIndicator.top = new FormAttachment(0, 10);
    fdSourceIndicator.width = 12; // Fixed width
    fdSourceIndicator.height = 12; // Fixed height
    sourceIndicator.setLayoutData(fdSourceIndicator);

    // Add a PaintListener to draw a border for better visibility
    sourceIndicator.addPaintListener(
        e -> {
          org.eclipse.swt.graphics.Rectangle bounds = sourceIndicator.getBounds();
          e.gc.setForeground(guiResource.getColorDarkGray());
          e.gc.setLineWidth(1);
          e.gc.drawRectangle(0, 0, bounds.width - 1, bounds.height - 1);
        });

    // Tooltip with source name and URL (source name shown in tooltip, not as text)
    String tooltipText = buildSourceTooltip(notification);
    sourceIndicator.setToolTipText(tooltipText);

    // Title - use CLabel for automatic ellipsis truncation
    // Title starts after source indicator
    CLabel titleLabel = new CLabel(composite, SWT.LEFT);
    String fullTitle = notification.getTitle() != null ? notification.getTitle() : "";
    titleLabel.setText(fullTitle);
    titleLabel.setData("type", "title"); // Mark for later updates
    titleLabel.setData("fullTitle", fullTitle); // Store full title for tooltip
    PropsUi.setLook(titleLabel);
    if (!notification.isRead()) {
      titleLabel.setFont(guiResource.getFontBold());
    }
    // Set tooltip to show full title if truncated
    titleLabel.setToolTipText(fullTitle);
    FormData fdTitle = new FormData();
    // Title starts after source indicator with 8px gap
    fdTitle.left = new FormAttachment(sourceIndicator, 8);
    // Title extends to right edge with margin
    fdTitle.right = new FormAttachment(100, -10);
    fdTitle.top = new FormAttachment(0, 10);
    // Don't attach bottom - CLabel will size to its preferred height
    titleLabel.setLayoutData(fdTitle);

    // Timestamp between title and body (always visible)
    Label timeLabel = new Label(composite, SWT.NONE);
    String timeText = formatTimestamp(notification.getTimestamp());
    if (timeText == null || timeText.isEmpty()) {
      timeText = "Unknown date";
    }
    timeLabel.setText(timeText);
    PropsUi.setLook(timeLabel);
    timeLabel.setForeground(guiResource.getColorDarkGray());
    FormData fdTime = new FormData();
    fdTime.left = new FormAttachment(priorityBar, 10);
    // Timestamp extends to right edge of composite
    fdTime.right = new FormAttachment(100, -10);
    fdTime.top = new FormAttachment(titleLabel, 5);
    // Don't attach bottom - label will size to its preferred height
    timeLabel.setLayoutData(fdTime);

    // Message/Description - simplified, truncated to max 3-5 lines
    Label messageLabel = null;
    String message = notification.getMessage();

    if (message != null && !message.isEmpty()) {
      // Limit to approximately 3-5 lines (roughly 200-300 characters)
      // Simple truncation - just show start of message
      int maxLength = 250;
      String displayMessage = message;
      if (displayMessage.length() > maxLength) {
        displayMessage = displayMessage.substring(0, maxLength).trim() + "...";
      }
      messageLabel = new Label(composite, SWT.WRAP);
      messageLabel.setText(displayMessage);
      PropsUi.setLook(messageLabel);
      FormData fdMessage = new FormData();
      fdMessage.left = new FormAttachment(priorityBar, 10);
      // Message extends to right edge of composite (sourceIndicator is on left, so don't constrain
      // by it)
      fdMessage.right = new FormAttachment(100, -10);
      fdMessage.top = new FormAttachment(timeLabel, 5);
      // Don't attach bottom - label will wrap and size to its content
      messageLabel.setLayoutData(fdMessage);

      // Set default cursor (not clickable)
      messageLabel.setCursor(
          composite.getDisplay().getSystemCursor(org.eclipse.swt.SWT.CURSOR_ARROW));
    }

    // Force layout of this composite to ensure all children are properly sized
    composite.layout(true, true);

    // Set cursor to pointer to indicate clickability for entire notification area
    // Store reference to notification ID and guiResource for updates
    composite.setData("notificationId", notification.getId());
    composite.setData("guiResource", guiResource);

    // Set cursor behavior: title and composite are clickable, body and timestamp are not
    org.eclipse.swt.graphics.Cursor handCursor =
        composite.getDisplay().getSystemCursor(org.eclipse.swt.SWT.CURSOR_HAND);
    org.eclipse.swt.graphics.Cursor defaultCursor =
        composite.getDisplay().getSystemCursor(org.eclipse.swt.SWT.CURSOR_ARROW);

    // Title is clickable - use hand cursor
    titleLabel.setCursor(handCursor);

    // Body (message) and timestamp are NOT clickable - use default cursor
    if (messageLabel != null) {
      messageLabel.setCursor(defaultCursor);
    }
    timeLabel.setCursor(defaultCursor);

    // Composite itself is clickable (for clicking outside title but still on notification)
    composite.setCursor(handCursor);

    // Click handler - attach to composite and all child controls
    org.eclipse.swt.widgets.Listener clickListener =
        e -> {
          // Mark as read first - this updates the notification in the service
          NotificationService.getInstance().markAsRead(notification.getId());

          // Get fresh notification from service to ensure we have updated state
          List<Notification> allNotifications =
              NotificationService.getInstance().getNotifications(false);
          Notification updatedNotification =
              allNotifications.stream()
                  .filter(n -> notification.getId().equals(n.getId()))
                  .findFirst()
                  .orElse(notification);

          // Ensure it's marked as read (should already be, but be safe)
          updatedNotification.setRead(true);

          // Update visual state immediately
          updateNotificationBackground(composite, updatedNotification, guiResource);

          // Force redraw to ensure visual changes are visible
          composite.redraw();

          // Open link if available
          if (updatedNotification.getLink() != null && !updatedNotification.getLink().isEmpty()) {
            try {
              EnvironmentUtils.getInstance().openUrl(updatedNotification.getLink());
            } catch (Exception ex) {
              // Silently ignore URL opening errors
            }
          }
        };

    // Attach click handler to composite
    composite.addListener(SWT.MouseDown, clickListener);

    // Also attach to all child controls to make entire area clickable
    attachClickListenerRecursive(composite, clickListener);

    return composite;
  }

  /** Update notification background based on read state and priority */
  private void updateNotificationBackground(
      Composite composite, Notification notification, GuiResource guiResource) {
    if (notification.isRead()) {
      // Read notifications have default background
      composite.setBackground(null);
    } else {
      // Unread notifications have colored background based on priority
      if (notification.getPriority() != null) {
        switch (notification.getPriority()) {
          case ERROR:
            composite.setBackground(guiResource.getColor(255, 240, 240)); // Light red tint
            break;
          case WARNING:
            composite.setBackground(guiResource.getColor(255, 250, 240)); // Light yellow tint
            break;
          case INFO:
          default:
            composite.setBackground(guiResource.getColorLightGray());
            break;
        }
      } else {
        composite.setBackground(guiResource.getColorLightGray());
      }
    }

    // Update priority bar color based on read state
    Control[] children = composite.getChildren();
    for (Control child : children) {
      if (child instanceof Composite) {
        Object type = child.getData("type");
        if ("priorityBar".equals(type)) {
          updatePriorityBar((Composite) child, notification, guiResource);
        }
      }
      // Check for title label (can be CLabel or Label)
      Object type = child.getData("type");
      if ("title".equals(type)) {
        if (child instanceof CLabel) {
          CLabel titleLabel = (CLabel) child;
          if (notification.isRead()) {
            // Use default font (remove bold)
            titleLabel.setFont(null);
          } else {
            titleLabel.setFont(guiResource.getFontBold());
          }
        } else if (child instanceof Label) {
          Label titleLabel = (Label) child;
          if (notification.isRead()) {
            // Use default font (remove bold)
            titleLabel.setFont(null);
          } else {
            titleLabel.setFont(guiResource.getFontBold());
          }
        }
      }
    }
  }

  /** Update priority bar color based on read state */
  private void updatePriorityBar(
      Composite priorityBar, Notification notification, GuiResource guiResource) {
    if (notification.isRead()) {
      // Read notifications have gray priority bar
      priorityBar.setBackground(guiResource.getColorGray());
    } else {
      // Unread notifications use the source color from configuration
      org.eclipse.swt.graphics.Color sourceColor = getSourceColor(notification, guiResource);
      priorityBar.setBackground(sourceColor);
    }
  }

  /** Recursively attach click listener to composite and all its children */
  private void attachClickListenerRecursive(
      Control control, org.eclipse.swt.widgets.Listener listener) {
    if (control == null || control.isDisposed()) {
      return;
    }
    // Don't attach to the priority bar or source indicator (they're just visual indicators)
    Object type = control.getData("type");
    if (!"priorityBar".equals(type) && !"sourceIndicator".equals(type)) {
      // Only attach to leaf controls (Labels, etc.) to avoid duplicate events
      // The composite already has the listener, so we don't need to attach to child composites
      if (!(control instanceof Composite)) {
        control.addListener(SWT.MouseDown, listener);
      }
    }
    if (control instanceof Composite) {
      Composite composite = (Composite) control;
      for (Control child : composite.getChildren()) {
        attachClickListenerRecursive(child, listener);
      }
    }
  }

  /** Recursively set cursor on composite and all its children */
  private void setCursorRecursive(Control control, org.eclipse.swt.graphics.Cursor cursor) {
    if (control == null || control.isDisposed()) {
      return;
    }
    control.setCursor(cursor);
    if (control instanceof Composite) {
      Composite composite = (Composite) control;
      for (Control child : composite.getChildren()) {
        setCursorRecursive(child, cursor);
      }
    }
  }

  /**
   * Get color for a notification source. This will be configurable via ConfigOption later. For now,
   * uses a simple hash-based color scheme.
   */
  private org.eclipse.swt.graphics.Color getSourceColor(
      Notification notification, GuiResource guiResource) {
    // Try to get color from notification source configuration
    String sourceId = notification.getSourceId();
    if (sourceId != null && !sourceId.isEmpty()) {
      try {
        org.apache.hop.ui.hopgui.notifications.config.NotificationConfigPlugin configPlugin =
            org.apache.hop.ui.hopgui.notifications.config.NotificationConfigPlugin.getInstance();
        java.util.List<org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig>
            sources = configPlugin.getSources();
        if (sources != null) {
          for (org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig source :
              sources) {
            if (sourceId.equals(source.getId())) {
              String colorHex = source.getColor();
              if (colorHex != null && !colorHex.isEmpty()) {
                try {
                  // Parse hex color (e.g., "#FF5733" or "FF5733")
                  String hex = colorHex.startsWith("#") ? colorHex.substring(1) : colorHex;
                  int colorValue = Integer.parseInt(hex, 16);
                  int r = (colorValue >> 16) & 0xFF;
                  int g = (colorValue >> 8) & 0xFF;
                  int b = colorValue & 0xFF;
                  return guiResource.getColor(r, g, b);
                } catch (NumberFormatException e) {
                  // Invalid hex color, fall through to hash-based color
                }
              }
              break;
            }
          }
        }
      } catch (Exception e) {
        // If lookup fails, fall through to hash-based color
      }
    }

    // Fallback: hash-based color generation for consistent colors per source
    String source = notification.getSource();
    if (source == null || source.isEmpty()) {
      return guiResource.getColorGray();
    }

    int hash = source.hashCode();
    int r = Math.abs(hash % 200) + 50; // 50-250 range
    int g = Math.abs((hash >> 8) % 200) + 50;
    int b = Math.abs((hash >> 16) % 200) + 50;

    return guiResource.getColor(r, g, b);
  }

  /** Build tooltip text for source indicator showing source name and URL */
  private String buildSourceTooltip(Notification notification) {
    StringBuilder tooltip = new StringBuilder();
    if (notification.getSource() != null && !notification.getSource().isEmpty()) {
      tooltip.append("Source: ").append(notification.getSource());
    }
    if (notification.getLink() != null && !notification.getLink().isEmpty()) {
      if (tooltip.length() > 0) {
        tooltip.append("\n");
      }
      tooltip.append("URL: ").append(notification.getLink());
    }
    return tooltip.length() > 0 ? tooltip.toString() : "Unknown source";
  }

  /** Format timestamp for display */
  private String formatTimestamp(Date timestamp) {
    if (timestamp == null) {
      return "";
    }
    long diff = System.currentTimeMillis() - timestamp.getTime();
    long minutes = diff / 60000;
    long hours = diff / 3600000;
    long days = diff / 86400000;

    if (minutes < 1) {
      return "Just now";
    } else if (minutes < 60) {
      return minutes + " minute" + (minutes > 1 ? "s" : "") + " ago";
    } else if (hours < 24) {
      return hours + " hour" + (hours > 1 ? "s" : "") + " ago";
    } else if (days < 7) {
      return days + " day" + (days > 1 ? "s" : "") + " ago";
    } else {
      return new SimpleDateFormat("MMM d, yyyy").format(timestamp);
    }
  }

  /** Position the panel below the bell icon */
  private void positionPanel() {
    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui != null && hopGui.getNotificationToolbarWidgets() != null) {
        org.apache.hop.ui.core.gui.GuiToolbarWidgets widgets =
            hopGui.getNotificationToolbarWidgets();

        org.eclipse.swt.graphics.Rectangle itemBounds = null;
        org.eclipse.swt.graphics.Point bottomLeftDisplay = null;

        // Desktop: bell is a ToolItem, parent is ToolBar
        org.eclipse.swt.widgets.ToolItem bellItem =
            widgets.findToolItem(NotificationToolbarItem.ID_NOTIFICATION_BELL);
        if (bellItem != null && !bellItem.isDisposed()) {
          org.eclipse.swt.widgets.ToolBar toolbar = bellItem.getParent();
          if (toolbar != null && !toolbar.isDisposed()) {
            itemBounds = bellItem.getBounds();
            bottomLeftDisplay =
                toolbar.toDisplay(itemBounds.x, itemBounds.y + itemBounds.height + 2);
          }
        }

        // Hop Web (RAP): bell is a Control (Composite) in widgetsMap
        if (bottomLeftDisplay == null) {
          org.eclipse.swt.widgets.Control bellControl =
              widgets.getControlForMenu(NotificationToolbarItem.ID_NOTIFICATION_BELL);
          if (bellControl != null && !bellControl.isDisposed()) {
            itemBounds = bellControl.getBounds();
            bottomLeftDisplay = bellControl.toDisplay(0, itemBounds.height + 2);
          }
        }

        if (bottomLeftDisplay != null && itemBounds != null) {
          org.eclipse.swt.graphics.Point panelSize = shell.getSize();
          int x = bottomLeftDisplay.x + itemBounds.width - panelSize.x;
          shell.setLocation(x, bottomLeftDisplay.y);
          return;
        }
      }
    } catch (Exception e) {
      // Fall through to default positioning
    }

    // Fallback: position in top-right corner
    if (parentShell != null && !parentShell.isDisposed()) {
      Point shellSize = parentShell.getSize();
      Point panelSize = shell.getSize();
      shell.setLocation(shellSize.x - panelSize.x - 20, 60);
    }
  }

  @Override
  public void notificationsChanged() {
    if (shell != null && !shell.isDisposed()) {
      Display.getCurrent()
          .asyncExec(
              () -> {
                if (shell != null && !shell.isDisposed() && isVisible) {
                  updateNotifications();
                }
              });
    }
  }

  /** Dispose the panel */
  public void dispose() {
    if (shell != null && !shell.isDisposed()) {
      shell.dispose();
    }
    NotificationService.getInstance().removeNotificationListener(this);
    instance = null;
  }
}
