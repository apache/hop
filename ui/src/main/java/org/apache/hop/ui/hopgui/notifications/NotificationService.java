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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig;

/** Service for managing notifications */
public class NotificationService {
  private static NotificationService instance;

  private static final String AUDIT_TYPE_READ_STATE = "notifications-read-state";
  private static final String AUDIT_TYPE_REMOVED_IDS = "notifications-removed-ids";
  private static final String CONFIG_KEY_ENABLED = "notification.system.enabled";
  private static final String CONFIG_KEY_SOURCES = "notification.sources";

  private final ILogChannel log;
  private final Map<String, INotificationProvider> providers;
  private final List<Notification> notifications;
  private final List<INotificationListener> listeners;
  private ScheduledExecutorService scheduler;
  private final Map<String, ScheduledFuture<?>> scheduledTasks;
  private final Map<String, Boolean> persistedReadState; // notificationId -> read state
  private final Set<String> persistedRemovedIds; // notification IDs that have been removed
  private final Map<String, ProviderErrorInfo> providerErrors; // providerId -> last error

  private NotificationService() {
    this.log = new LogChannel("NotificationService");
    this.providers = new ConcurrentHashMap<>();
    this.notifications = new CopyOnWriteArrayList<>();
    this.listeners = new CopyOnWriteArrayList<>();
    this.scheduledTasks = new ConcurrentHashMap<>();
    this.persistedReadState = new ConcurrentHashMap<>();
    this.persistedRemovedIds = ConcurrentHashMap.newKeySet();
    this.providerErrors = new ConcurrentHashMap<>();
    loadPersistedReadState();
    loadPersistedRemovedState();
  }

  public static synchronized NotificationService getInstance() {
    if (instance == null) {
      instance = new NotificationService();
    }
    return instance;
  }

  /**
   * Register a notification provider
   *
   * @param provider The provider to register
   */
  public void registerProvider(INotificationProvider provider) {
    if (provider == null || provider.getId() == null) {
      log.logError("Cannot register null provider or provider without ID");
      return;
    }

    providers.put(provider.getId(), provider);
    log.logDetailed("Registered notification provider: " + provider.getName());

    // Schedule polling if scheduler is running
    if (scheduler != null && !scheduler.isShutdown()) {
      scheduleProviderPolling(provider);
    }
  }

  /**
   * Get a notification provider by ID
   *
   * @param providerId The ID of the provider
   * @return The provider, or null if not found
   */
  public INotificationProvider getProvider(String providerId) {
    return providers.get(providerId);
  }

  /**
   * Unregister a notification provider
   *
   * @param providerId The ID of the provider to unregister
   */
  public void unregisterProvider(String providerId) {
    providerErrors.remove(providerId);
    INotificationProvider provider = providers.remove(providerId);
    if (provider != null) {
      // Cancel scheduled polling
      ScheduledFuture<?> task = scheduledTasks.remove(providerId);
      if (task != null) {
        task.cancel(false);
      }
      try {
        provider.shutdown();
      } catch (Exception e) {
        log.logError("Error shutting down provider " + provider.getName(), e);
      }
      log.logDetailed("Unregistered notification provider: " + provider.getName());
    }
  }

  /**
   * Reschedule polling for an existing provider. Use when poll interval or enabled state changed
   * (e.g. CUSTOM_PLUGIN config update). Cancels the current task and schedules a new one.
   *
   * @param provider The provider to reschedule
   */
  public void rescheduleProvider(INotificationProvider provider) {
    if (provider == null || provider.getId() == null) {
      return;
    }
    ScheduledFuture<?> existing = scheduledTasks.remove(provider.getId());
    if (existing != null) {
      existing.cancel(false);
    }
    if (provider.isEnabled() && scheduler != null && !scheduler.isShutdown()) {
      scheduleProviderPolling(provider);
    }
  }

  /**
   * Reload providers from HopConfig and sync with current in-memory state. Called when the user
   * saves notification settings. Ensures provider lifecycle: unregisters removed/disabled sources,
   * registers new ones, updates CUSTOM_PLUGIN settings. Existing notifications are left as-is.
   */
  public void reloadFromConfig() {
    synchronized (this) {
      try {
        boolean enabled =
            HopConfig.readOptionString(CONFIG_KEY_ENABLED, "true").equalsIgnoreCase("true");
        if (!enabled) {
          log.logDetailed("Notification system disabled, stopping service");
          stop();
          return;
        }

        List<NotificationSourceConfig> sources = loadSourcesFromConfig();
        Set<String> desiredIds = new HashSet<>();
        for (NotificationSourceConfig s : sources) {
          if (s.isEnabled()) {
            desiredIds.add(s.getId());
            // CUSTOM_PLUGIN providers are registered under pluginId; getPluginId() falls back to id
            if (s.getType() == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
              String pluginId = s.getPluginId();
              if (!Utils.isEmpty(pluginId)) {
                desiredIds.add(pluginId);
              }
            }
          }
        }

        // Unregister providers no longer in config or disabled
        List<String> toRemove = new ArrayList<>();
        for (String id : providers.keySet()) {
          if (!desiredIds.contains(id)) {
            toRemove.add(id);
          }
        }
        for (String id : toRemove) {
          unregisterProvider(id);
        }

        // Sync each desired source
        int registered = 0;
        int updated = 0;
        int failed = 0;

        for (NotificationSourceConfig source : sources) {
          if (!source.isEnabled()) {
            continue;
          }
          String id = source.getId();
          if (id == null || id.isEmpty()) {
            continue;
          }

          if (source.getType() == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
            String pluginId = source.getPluginId();
            if (Utils.isEmpty(pluginId)) {
              pluginId = id;
            }
            INotificationProvider existing = getProvider(pluginId);
            if (existing != null) {
              long pollIntervalMs = parsePollIntervalMs(source.getPollIntervalMinutes());
              existing.setPollInterval(pollIntervalMs);
              existing.setEnabled(true);
              rescheduleProvider(existing);
              updated++;
            } else {
              log.logDetailed(
                  "Custom plugin provider '"
                      + pluginId
                      + "' not found (plugin may not have loaded yet)");
            }
            continue;
          }

          // GitHub / RSS: create fresh provider from config
          unregisterProvider(id); // Replace if exists
          INotificationProvider provider = NotificationProviderFactory.createProvider(source, log);
          if (provider != null) {
            try {
              provider.initialize();
              registerProvider(provider);
              registered++;
            } catch (Exception e) {
              log.logError("Error initializing provider '" + source.getName() + "'", e);
              failed++;
            }
          } else {
            failed++;
          }
        }

        // Restart scheduler if it was stopped (e.g. after re-enabling)
        if (scheduler == null || scheduler.isShutdown()) {
          start();
        }

        log.logDetailed(
            "Notification config reloaded: "
                + registered
                + " registered, "
                + updated
                + " updated, "
                + failed
                + " failed");
      } catch (Exception e) {
        log.logError("Error reloading notification config", e);
      }
    }
  }

  private List<NotificationSourceConfig> loadSourcesFromConfig() {
    try {
      String json = HopConfig.readOptionString(CONFIG_KEY_SOURCES, null);
      if (!Utils.isEmpty(json)) {
        ObjectMapper mapper = JsonUtil.jsonMapper();
        return mapper.readValue(json, new TypeReference<List<NotificationSourceConfig>>() {});
      }
    } catch (Exception e) {
      log.logError("Error loading notification sources from config", e);
    }
    return new ArrayList<>();
  }

  private long parsePollIntervalMs(String value) {
    if (Utils.isEmpty(value)) {
      return 3600000;
    }
    try {
      int minutes = Integer.parseInt(value.trim());
      return minutes > 0 ? minutes * 60L * 1000L : 3600000;
    } catch (NumberFormatException e) {
      return 3600000;
    }
  }

  /**
   * Get all notifications
   *
   * @param unreadOnly If true, only return unread notifications
   * @return List of notifications sorted by date (descending - newest first)
   */
  public List<Notification> getNotifications(boolean unreadOnly) {
    return getNotifications(unreadOnly, 0);
  }

  /**
   * Get all notifications
   *
   * @param unreadOnly If true, only return unread notifications
   * @param daysToGoBack Number of days to go back (0 = no limit)
   * @return List of notifications sorted by date (descending - newest first)
   */
  public List<Notification> getNotifications(boolean unreadOnly, int daysToGoBack) {
    List<Notification> result;
    if (unreadOnly) {
      result = notifications.stream().filter(n -> !n.isRead()).collect(Collectors.toList());
      log.logDetailed(
          "Filtered to unread only: " + result.size() + " out of " + notifications.size());
    } else {
      result = new ArrayList<>(notifications);
    }

    // Filter by days to go back if specified
    if (daysToGoBack > 0) {
      long cutoffTime = System.currentTimeMillis() - (daysToGoBack * 24L * 60L * 60L * 1000L);
      Date cutoffDate = new Date(cutoffTime);
      int beforeFilter = result.size();
      result =
          result.stream()
              .filter(
                  n -> {
                    Date timestamp = n.getTimestamp();
                    if (timestamp == null) {
                      return false; // Exclude notifications without timestamps
                    }
                    return timestamp.after(cutoffDate);
                  })
              .collect(Collectors.toList());
      log.logDetailed(
          "Filtered by daysToGoBack ("
              + daysToGoBack
              + " days): "
              + result.size()
              + " out of "
              + beforeFilter
              + " (cutoff: "
              + cutoffDate
              + ")");
    }

    // Sort by timestamp descending (newest first)
    result.sort(
        (n1, n2) -> {
          Date d1 = n1.getTimestamp();
          Date d2 = n2.getTimestamp();
          if (d1 == null && d2 == null) {
            return 0;
          }
          if (d1 == null) {
            return 1; // null dates go to end
          }
          if (d2 == null) {
            return -1;
          }
          return d2.compareTo(d1); // Descending order
        });

    return result;
  }

  /**
   * Get count of unread notifications
   *
   * @return Number of unread notifications
   */
  public int getUnreadCount() {
    return (int) notifications.stream().filter(n -> !n.isRead()).count();
  }

  /**
   * Get total count of all notifications (for debugging)
   *
   * @return Total number of notifications in the service
   */
  public int getTotalCount() {
    return notifications.size();
  }

  /**
   * Mark a notification as read
   *
   * @param notificationId The ID of the notification to mark as read
   */
  public void markAsRead(String notificationId) {
    notifications.stream()
        .filter(n -> notificationId.equals(n.getId()))
        .forEach(n -> n.setRead(true));
    // Persist read state
    persistedReadState.put(notificationId, true);
    savePersistedReadState();
    notifyListeners();
  }

  /**
   * Clear all notifications: mark as read, remove from panel, and persist as read+removed so they
   * don't reappear on future fetches.
   */
  public void clearAll() {
    // Mark all as read first
    notifications.forEach(
        n -> {
          n.setRead(true);
          persistedReadState.put(n.getId(), true);
          persistedRemovedIds.add(n.getId());
        });
    notifications.clear();
    savePersistedReadState();
    savePersistedRemovedState();
    notifyListeners();
    log.logDetailed("Cleared all notifications");
  }

  /** Mark all notifications as read */
  public void markAllAsRead() {
    notifications.forEach(
        n -> {
          n.setRead(true);
          persistedReadState.put(n.getId(), true);
        });
    savePersistedReadState();
    notifyListeners();
  }

  /**
   * Add a notification
   *
   * @param notification The notification to add
   */
  public void addNotification(Notification notification) {
    if (notification == null || notification.getId() == null) {
      return;
    }

    // Don't re-add notifications that were removed by the user
    if (persistedRemovedIds.contains(notification.getId())) {
      return;
    }

    // Check for duplicates
    boolean exists = notifications.stream().anyMatch(n -> notification.getId().equals(n.getId()));

    if (!exists) {
      // Apply persisted read state if available
      Boolean readState = persistedReadState.get(notification.getId());
      if (readState != null) {
        notification.setRead(readState);
      }
      notifications.add(notification);
      notifyListeners();
      log.logDetailed("Added notification: " + notification.getTitle());
    }
  }

  /**
   * Remove a notification
   *
   * @param notificationId The ID of the notification to remove
   */
  public void removeNotification(String notificationId) {
    notifications.removeIf(n -> notificationId.equals(n.getId()));
    notifyListeners();
  }

  /**
   * Add a listener for notification changes
   *
   * @param listener The listener to add
   */
  public void addNotificationListener(INotificationListener listener) {
    if (listener != null) {
      listeners.add(listener);
    }
  }

  /**
   * Remove a listener
   *
   * @param listener The listener to remove
   */
  public void removeNotificationListener(INotificationListener listener) {
    listeners.remove(listener);
  }

  /** Notify all listeners of changes */
  private void notifyListeners() {
    for (INotificationListener listener : listeners) {
      try {
        listener.notificationsChanged();
      } catch (Exception e) {
        log.logError("Error notifying listener", e);
      }
    }
  }

  /**
   * Fetch notifications from all enabled providers
   *
   * @throws HopException if there's an error
   */
  public void fetchFromProviders() throws HopException {
    for (INotificationProvider provider : providers.values()) {
      if (!provider.isEnabled()) {
        continue;
      }

      try {
        List<Notification> fetched = provider.fetchNotifications();
        for (Notification notification : fetched) {
          addNotification(notification);
        }
        clearProviderError(provider.getId());
      } catch (Exception e) {
        log.logError("Error fetching notifications from provider: " + provider.getName(), e);
        recordProviderError(provider.getId(), provider.getName(), e);
      }
    }
  }

  /**
   * Retry fetching from all providers. Clears error state and triggers a fresh fetch. Call from UI
   * when user clicks "Retry" on the provider error banner.
   */
  public void retryNow() {
    try {
      fetchFromProviders();
      notifyListeners();
    } catch (Exception e) {
      log.logError("Error during retry", e);
      notifyListeners();
    }
  }

  /**
   * Get provider errors for UI display. Returns a copy of the current error list.
   *
   * @return List of provider errors (may be empty)
   */
  public List<ProviderErrorInfo> getProviderErrors() {
    return new ArrayList<>(providerErrors.values());
  }

  private void recordProviderError(String providerId, String providerName, Throwable e) {
    String message = e.getMessage();
    if (message == null || message.isEmpty()) {
      message = e.getClass().getSimpleName();
    }
    boolean hadError = providerErrors.containsKey(providerId);
    providerErrors.put(
        providerId, new ProviderErrorInfo(providerId, providerName, message, new Date()));
    if (!hadError) {
      notifyListeners();
    }
  }

  private void clearProviderError(String providerId) {
    if (providerErrors.remove(providerId) != null) {
      notifyListeners();
    }
  }

  /** Start the notification service */
  public void start() {
    log.logDetailed("Starting notification service");

    // Initialize scheduler
    if (scheduler == null || scheduler.isShutdown()) {
      scheduler = Executors.newScheduledThreadPool(2);
    }

    // Initialize and schedule polling for all providers
    for (INotificationProvider provider : providers.values()) {
      try {
        provider.initialize();
        scheduleProviderPolling(provider);
      } catch (Exception e) {
        log.logError("Error initializing provider: " + provider.getName(), e);
      }
    }
  }

  /** Schedule periodic polling for a provider */
  private void scheduleProviderPolling(INotificationProvider provider) {
    if (!provider.isEnabled()) {
      return;
    }

    long pollInterval = provider.getPollInterval();
    if (pollInterval <= 0) {
      pollInterval = 3600000; // Default to 1 hour
    }

    // Cancel existing task if any
    ScheduledFuture<?> existingTask = scheduledTasks.get(provider.getId());
    if (existingTask != null) {
      existingTask.cancel(false);
    }

    // Schedule new polling task
    ScheduledFuture<?> task =
        scheduler.scheduleWithFixedDelay(
            () -> {
              try {
                if (provider.isEnabled()) {
                  List<Notification> fetched = provider.fetchNotifications();
                  for (Notification notification : fetched) {
                    addNotification(notification);
                  }
                  clearProviderError(provider.getId());
                }
              } catch (Exception e) {
                log.logError("Error polling provider: " + provider.getName(), e);
                recordProviderError(provider.getId(), provider.getName(), e);
              }
            },
            pollInterval, // Initial delay
            pollInterval, // Period
            TimeUnit.MILLISECONDS);

    scheduledTasks.put(provider.getId(), task);
    log.logDetailed(
        "Scheduled polling for provider "
            + provider.getName()
            + " every "
            + (pollInterval / 60000)
            + " minutes");
  }

  /** Stop the notification service */
  public void stop() {
    log.logDetailed("Stopping notification service");

    // Cancel all scheduled tasks
    for (ScheduledFuture<?> task : scheduledTasks.values()) {
      if (task != null) {
        task.cancel(false);
      }
    }
    scheduledTasks.clear();

    // Shutdown scheduler
    if (scheduler != null && !scheduler.isShutdown()) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    // Shutdown providers
    for (INotificationProvider provider : providers.values()) {
      try {
        provider.shutdown();
      } catch (Exception e) {
        log.logError("Error shutting down provider: " + provider.getName(), e);
      }
    }
    listeners.clear();
  }

  /** Load persisted read state from audit manager */
  private void loadPersistedReadState() {
    try {
      IAuditManager auditManager = AuditManager.getActive();
      if (auditManager == null) {
        return; // Audit manager not available yet
      }
      // Use global Hop GUI namespace - notifications are installation-wide, not project-specific
      String namespace = HopGui.DEFAULT_HOP_GUI_NAMESPACE;
      Map<String, String> readStateMap = auditManager.loadMap(namespace, AUDIT_TYPE_READ_STATE);
      if (readStateMap != null) {
        for (Map.Entry<String, String> entry : readStateMap.entrySet()) {
          String notificationId = entry.getKey();
          String readStateStr = entry.getValue();
          boolean isRead = "true".equalsIgnoreCase(readStateStr);
          persistedReadState.put(notificationId, isRead);
        }
        log.logDetailed(
            "Loaded persisted read state for " + persistedReadState.size() + " notification(s)");
      }
    } catch (Exception e) {
      log.logError("Error loading persisted notification read state", e);
    }
  }

  /** Save persisted read state to audit manager */
  private void savePersistedReadState() {
    try {
      IAuditManager auditManager = AuditManager.getActive();
      if (auditManager == null) {
        return; // Audit manager not available yet
      }
      // Use global Hop GUI namespace - notifications are installation-wide, not project-specific
      String namespace = HopGui.DEFAULT_HOP_GUI_NAMESPACE;
      Map<String, String> readStateMap = new java.util.HashMap<>();
      for (Map.Entry<String, Boolean> entry : persistedReadState.entrySet()) {
        readStateMap.put(entry.getKey(), String.valueOf(entry.getValue()));
      }
      auditManager.saveMap(namespace, AUDIT_TYPE_READ_STATE, readStateMap);
      log.logDetailed("Saved persisted read state for " + readStateMap.size() + " notification(s)");
    } catch (Exception e) {
      log.logError("Error saving persisted notification read state", e);
    }
  }

  /** Load persisted removed IDs from audit manager */
  private void loadPersistedRemovedState() {
    try {
      IAuditManager auditManager = AuditManager.getActive();
      if (auditManager == null) {
        return;
      }
      String namespace = HopGui.DEFAULT_HOP_GUI_NAMESPACE;
      Map<String, String> removedMap = auditManager.loadMap(namespace, AUDIT_TYPE_REMOVED_IDS);
      if (removedMap != null) {
        persistedRemovedIds.addAll(removedMap.keySet());
        log.logDetailed(
            "Loaded persisted removed state for "
                + persistedRemovedIds.size()
                + " notification(s)");
      }
    } catch (Exception e) {
      log.logError("Error loading persisted notification removed state", e);
    }
  }

  /** Save persisted removed IDs to audit manager */
  private void savePersistedRemovedState() {
    try {
      IAuditManager auditManager = AuditManager.getActive();
      if (auditManager == null) {
        return;
      }
      String namespace = HopGui.DEFAULT_HOP_GUI_NAMESPACE;
      Map<String, String> removedMap = new java.util.HashMap<>();
      for (String id : persistedRemovedIds) {
        removedMap.put(id, "removed");
      }
      auditManager.saveMap(namespace, AUDIT_TYPE_REMOVED_IDS, removedMap);
      log.logDetailed(
          "Saved persisted removed state for " + removedMap.size() + " notification(s)");
    } catch (Exception e) {
      log.logError("Error saving persisted notification removed state", e);
    }
  }
}
