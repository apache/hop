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
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ISingletonProvider;
import org.apache.hop.ui.hopgui.ImplementationLoader;
import org.apache.hop.ui.hopgui.notifications.config.NotificationSourceConfig;

/** Service for managing notifications */
public class NotificationService {
  private static NotificationService fallback;

  private static final ISingletonProvider PROVIDER = loadProvider();

  private static ISingletonProvider loadProvider() {
    try {
      return (ISingletonProvider) ImplementationLoader.newInstance(NotificationService.class);
    } catch (Throwable e) {
      // hop-ui unit tests have no rcp/rap *Impl on the classpath. Anywhere else this is a
      // misconfiguration worth shouting about: one instance would then be shared by every Hop Web
      // session, which is the very thing the per-session provider exists to prevent.
      LogChannel.GENERAL.logBasic(
          "No NotificationServiceImpl found; falling back to a single instance for this process. "
              + "In Hop Web that means every session shares one.");
      return () -> {
        synchronized (NotificationService.class) {
          if (fallback == null) {
            fallback = new NotificationService();
          }
          return fallback;
        }
      };
    }
  }

  private static final String AUDIT_TYPE_READ_STATE = "notifications-read-state";
  private static final String AUDIT_TYPE_REMOVED_IDS = "notifications-removed-ids";
  private static final String CONFIG_KEY_ENABLED = "notification.system.enabled";
  private static final String CONFIG_KEY_SOURCES = "notification.sources";

  /**
   * How long after the service starts the first poll runs. Startup used to fetch synchronously on
   * the UI thread; polling now covers the first fetch too, just soon rather than a poll interval
   * away.
   */
  private static final long INITIAL_FETCH_DELAY_MS = 5000;

  /** Keep the provider error readable on the single-line banner in the notification panel. */
  private static final int MAX_ERROR_MESSAGE_LENGTH = 200;

  /** Separates the source from the provider's own identifier in a notification id. */
  private static final String ID_SEPARATOR = ":";

  /**
   * How many notifications are kept in memory. Every one of them becomes a row of widgets when the
   * panel opens, and a busy feed would otherwise grow the list for as long as Hop is running.
   */
  private static final int MAX_NOTIFICATIONS = 500;

  /**
   * How many read and removed identifiers are remembered. The state is what stops a notification
   * coming back, so it has to outlive the notification itself, but it cannot grow without limit:
   * the oldest entries are dropped once the cap is reached.
   */
  private static final int MAX_PERSISTED_IDS = 2000;

  private final ILogChannel log;
  private final Map<String, INotificationProvider> providers;
  private final List<Notification> notifications;
  private final List<INotificationListener> listeners;
  private ScheduledExecutorService scheduler;
  private final Map<String, ScheduledFuture<?>> scheduledTasks;
  private final Map<String, Long> persistedReadState; // notificationId -> when it was read
  private final Map<String, Long> persistedRemovedIds; // notificationId -> when it was removed
  private final Map<String, ProviderErrorInfo> providerErrors; // providerId -> last error

  /**
   * Set while the service is being stopped. Stopping interrupts whatever poll is in flight, and the
   * source it was talking to then fails on a closed socket. That is the service shutting the poll
   * down, not the source being unreachable, so it is not worth an error banner.
   */
  private volatile boolean stopping;

  /**
   * Use {@link #getInstance()}. Public because Hop Web asks RWT to create one instance of this per
   * user session, and constructed directly by tests that want an isolated service.
   */
  public NotificationService() {
    this.log = new LogChannel("NotificationService");
    this.providers = new ConcurrentHashMap<>();
    this.notifications = new CopyOnWriteArrayList<>();
    this.listeners = new CopyOnWriteArrayList<>();
    this.scheduledTasks = new ConcurrentHashMap<>();
    this.persistedReadState = new ConcurrentHashMap<>();
    this.persistedRemovedIds = new ConcurrentHashMap<>();
    this.providerErrors = new ConcurrentHashMap<>();
    loadPersistedReadState();
    loadPersistedRemovedState();
  }

  /**
   * The notification service of this process, or of this user's session in Hop Web.
   *
   * <p>It holds what has been read, which sources are being polled and which panel is listening -
   * all of it belonging to one user. A single instance shared between Hop Web sessions would hand
   * one user's read state, and another session's widgets, to everybody.
   *
   * @return The service
   */
  public static NotificationService getInstance() {
    return (NotificationService) PROVIDER.getInstanceInternal();
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
    log.logBasic(
        "Registered notification source '" + provider.getName() + "' (" + provider.getId() + ")");

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
      removeNotificationsOf(providerId);
      log.logDetailed("Unregistered notification provider: " + provider.getName());
    }
  }

  /**
   * Drop what a provider contributed, now that it is gone.
   *
   * <p>A source that is deleted, disabled, or pointed somewhere else should not leave its
   * notifications sitting in the panel: they are about something the user has said they no longer
   * want to hear from. Their read state is left alone, so a source that comes back does not report
   * everything as new again.
   *
   * @param providerId The provider whose notifications should go
   */
  private void removeNotificationsOf(String providerId) {
    boolean removed = notifications.removeIf(n -> providerId.equals(n.getSourceId()));
    if (removed) {
      notifyListeners();
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

        // What should be registered after this reload. Sources the user configured, plus every
        // provider a plugin declares: those are discovered rather than configured, so the absence
        // of a source for one means "leave it alone", not "remove it". Only a stored source that
        // explicitly disables a plugin takes it out.
        Set<String> desiredIds = new HashSet<>();
        for (NotificationSourceConfig source : sources) {
          if (source.isEnabled()
              && source.getType() != NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
            desiredIds.add(source.getId());
          }
        }
        for (String pluginId : NotificationProviderPlugins.ids()) {
          NotificationSourceConfig override = findSource(sources, pluginId);
          if (override == null || override.isEnabled()) {
            desiredIds.add(pluginId);
          }
        }

        // Unregister providers no longer wanted
        List<String> toRemove = new ArrayList<>();
        for (String id : providers.keySet()) {
          if (!desiredIds.contains(id)) {
            toRemove.add(id);
          }
        }
        for (String id : toRemove) {
          unregisterProvider(id);
        }

        int registered = 0;
        int updated = 0;
        int failed = 0;

        // Providers declared by plugins: register the ones that are missing, and apply the poll
        // interval to the ones already running.
        for (IPlugin plugin : NotificationProviderPlugins.plugins()) {
          String pluginId = NotificationProviderPlugins.idOf(plugin);
          if (pluginId == null || !desiredIds.contains(pluginId)) {
            continue;
          }
          NotificationSourceConfig override = findSource(sources, pluginId);
          INotificationProvider existing = getProvider(pluginId);
          if (existing == null) {
            existing = NotificationProviderPlugins.load(plugin, log);
            if (existing == null) {
              failed++;
              continue;
            }
            applyPollInterval(existing, override);
            try {
              existing.initialize();
              registerProvider(existing);
              registered++;
            } catch (Exception e) {
              log.logError("Error initializing the notification provider of plugin " + pluginId, e);
              failed++;
            }
          } else {
            applyPollInterval(existing, override);
            existing.setEnabled(true);
            rescheduleProvider(existing);
            updated++;
          }
        }

        // Sources the user configured: rebuilt from their current settings.
        for (NotificationSourceConfig source : sources) {
          if (!source.isEnabled()
              || source.getType() == NotificationSourceConfig.SourceType.CUSTOM_PLUGIN) {
            continue;
          }
          String id = source.getId();
          if (Utils.isEmpty(id)) {
            continue;
          }
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

        log.logBasic(
            "Notification config reloaded: "
                + registered
                + " registered, "
                + updated
                + " updated, "
                + failed
                + " failed");

        // Sources that were replaced had their notifications dropped with them. Fetch again now,
        // in the background, rather than leaving the panel short until the next poll comes round.
        retryNow();
      } catch (Exception e) {
        log.logError("Error reloading notification config", e);
      }
    }
  }

  /**
   * Find the stored source for a plugin, by its plugin id or its own id.
   *
   * @param sources The configured sources
   * @param pluginId The plugin to look for
   * @return The source, or null when the plugin has never been configured
   */
  private NotificationSourceConfig findSource(
      List<NotificationSourceConfig> sources, String pluginId) {
    for (NotificationSourceConfig source : sources) {
      if (pluginId.equals(source.getPluginId()) || pluginId.equals(source.getId())) {
        return source;
      }
    }
    return null;
  }

  /**
   * Apply a configured poll interval to a provider, leaving the provider's own default in place
   * when the source does not set one.
   *
   * @param provider The provider to configure
   * @param source The stored source, may be null
   */
  private void applyPollInterval(INotificationProvider provider, NotificationSourceConfig source) {
    if (source != null && !Utils.isEmpty(source.getPollIntervalMinutes())) {
      provider.setPollInterval(parsePollIntervalMs(source.getPollIntervalMinutes()));
    }
  }

  private List<NotificationSourceConfig> loadSourcesFromConfig() {
    return org.apache.hop.ui.hopgui.notifications.config.NotificationSources.load();
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
    persistedReadState.put(notificationId, System.currentTimeMillis());
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
          long now = System.currentTimeMillis();
          persistedReadState.put(n.getId(), now);
          persistedRemovedIds.put(n.getId(), now);
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
          persistedReadState.put(n.getId(), System.currentTimeMillis());
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

    qualifyId(notification);

    // Don't re-add notifications that were removed by the user
    if (persistedRemovedIds.containsKey(notification.getId())) {
      return;
    }

    // Check for duplicates
    boolean exists = notifications.stream().anyMatch(n -> notification.getId().equals(n.getId()));

    if (!exists) {
      // Apply persisted read state if available
      if (persistedReadState.containsKey(notification.getId())) {
        notification.setRead(true);
      }
      sanitizeLink(notification);
      notifications.add(notification);
      trimToMaximum();
      notifyListeners();
      log.logDetailed("Added notification: " + notification.getTitle());
    }
  }

  /**
   * Qualify a provider's identifier with the source it came from.
   *
   * <p>A provider only has to number its notifications uniquely among its own, which is all it can
   * reasonably guarantee: it knows nothing about the other sources the user has configured. Read
   * state, removed state and duplicate detection are all keyed on the identifier, so two sources
   * that happen to name a notification the same way - two repositories releasing the same version
   * number, say - would otherwise be treated as one notification, and marking one read would mark
   * the other read too.
   *
   * <p>Rewriting the id in place means everything downstream, the panel included, works with one
   * identifier. Applying it twice is harmless, so a provider that hands back the same instances on
   * a later poll is not penalised.
   *
   * @param notification The notification to qualify, modified in place
   */
  private void qualifyId(Notification notification) {
    String sourceId = notification.getSourceId();
    if (Utils.isEmpty(sourceId)) {
      log.logBasic(
          "Notification '"
              + notification.getId()
              + "' does not name the source it came from, so it cannot be told apart from a"
              + " notification of the same name from another source");
      return;
    }
    String prefix = sourceId + ID_SEPARATOR;
    if (!notification.getId().startsWith(prefix)) {
      notification.setId(prefix + notification.getId());
    }
  }

  /**
   * Keep the retained notifications within {@link #MAX_NOTIFICATIONS}, dropping the oldest.
   *
   * <p>Read state is persisted separately, so a notification dropped here does not come back as
   * unread if the source still offers it.
   */
  private void trimToMaximum() {
    int excess = notifications.size() - MAX_NOTIFICATIONS;
    if (excess <= 0) {
      return;
    }
    List<Notification> oldestFirst = new ArrayList<>(notifications);
    oldestFirst.sort(
        (n1, n2) -> {
          Date d1 = n1.getTimestamp();
          Date d2 = n2.getTimestamp();
          if (d1 == null && d2 == null) {
            return 0;
          }
          if (d1 == null) {
            return -1;
          }
          if (d2 == null) {
            return 1;
          }
          return d1.compareTo(d2);
        });
    for (int i = 0; i < excess; i++) {
      notifications.remove(oldestFirst.get(i));
    }
    log.logDetailed("Dropped " + excess + " notification(s) over the retention limit");
  }

  /**
   * Drop the oldest entries of a persisted map once it grows past the cap.
   *
   * @param state The map of identifier to the moment it was recorded, modified in place
   * @param what What the map holds, for the log
   */
  private void prune(Map<String, Long> state, String what) {
    int excess = state.size() - MAX_PERSISTED_IDS;
    if (excess <= 0) {
      return;
    }
    List<Map.Entry<String, Long>> oldestFirst = new ArrayList<>(state.entrySet());
    oldestFirst.sort(Map.Entry.comparingByValue());
    for (int i = 0; i < excess; i++) {
      state.remove(oldestFirst.get(i).getKey());
    }
    log.logDetailed("Dropped " + excess + " " + what + " over the retention limit");
  }

  /**
   * Drop a link the notification panel must not open. The link comes from a remote feed and is
   * handed to the operating system when the notification is clicked, so anything that is not an
   * http(s) URL is discarded here rather than at the click.
   *
   * @param notification The notification to check
   */
  private void sanitizeLink(Notification notification) {
    String link = notification.getLink();
    if (link != null && !link.isEmpty() && !NotificationLinks.isSafe(link)) {
      log.logBasic(
          "Ignoring the link on notification '"
              + notification.getId()
              + "': only http and https links are opened");
      notification.setLink(null);
    }
  }

  /**
   * Take a notification out of the list, leaving its read state alone.
   *
   * <p>This drops it from what is held in memory; it does not record it as removed, so the source
   * offering it again brings it back - as read, if it was read. That is what the retention cap
   * wants. A dismiss button would want the opposite, the way {@link #clearAll()} does it, and
   * should say so rather than reusing this.
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
        log.logDetailed(
            "Notification source '" + provider.getName() + "' is disabled, not polling");
        continue;
      }
      fetchFrom(provider);
    }
  }

  /**
   * Poll one provider and take in whatever it offers.
   *
   * <p>Reports what happened at basic level. A source that is polled but offers nothing, and a
   * source that offers the same items it offered last time, look identical from the panel - both
   * leave it empty - so the log is the only place that tells them apart.
   *
   * @param provider The provider to poll
   */
  private void fetchFrom(INotificationProvider provider) {
    try {
      List<Notification> fetched = provider.fetchNotifications();
      int before = notifications.size();
      for (Notification notification : fetched) {
        addNotification(notification);
      }
      int added = notifications.size() - before;
      String outcome =
          "Polled notification source '"
              + provider.getName()
              + "' ("
              + provider.getId()
              + "): "
              + fetched.size()
              + " offered, "
              + added
              + " new";
      // Hop GUI is quiet by design, and a source is polled on a timer for as long as it runs.
      // Only a poll that changed something is worth a line at basic level.
      if (added > 0) {
        log.logBasic(outcome);
      } else {
        log.logDetailed(outcome);
      }
      clearProviderError(provider.getId());
    } catch (Exception e) {
      if (stopping) {
        log.logDetailed("Poll of '" + provider.getName() + "' cut short by the service stopping");
        return;
      }
      log.logError("Error fetching notifications from provider: " + provider.getName(), e);
      recordProviderError(provider.getId(), provider.getName(), e);
    }
  }

  /**
   * Retry fetching from all providers, on a background thread. Called from the UI when the user
   * clicks "Retry" on the provider error banner, so it must never block: a fetch talks to every
   * configured source over the network. Listeners are notified when the fetch is done, whether or
   * not anything changed, so the panel always reflects the attempt.
   */
  public void retryNow() {
    // Asking for a retry says the service is wanted, whatever it was doing before. Without this a
    // retry after the service was stopped would have its failures written off as the stop.
    stopping = false;
    runAsync(
        () -> {
          try {
            fetchFromProviders();
          } catch (Exception e) {
            log.logError("Error during retry", e);
          }
          notifyListeners();
        });
  }

  /**
   * Run a task off the calling thread. Uses the polling scheduler when the service is running, and
   * a one-shot daemon thread when it is not, so a caller on the UI thread is never blocked.
   *
   * @param task The task to run
   */
  private void runAsync(Runnable task) {
    ScheduledExecutorService currentScheduler = scheduler;
    if (currentScheduler != null && !currentScheduler.isShutdown()) {
      currentScheduler.execute(task);
      return;
    }
    Thread thread = new Thread(task, "hop-notifications-fetch");
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Scheduler for provider polling. The threads are daemons: nothing stops the service when a Hop
   * GUI shell closes, and in Hop Web there is no {@code System.exit()} to fall back on.
   *
   * @return A new scheduler
   */
  private static ScheduledExecutorService newScheduler() {
    return Executors.newScheduledThreadPool(
        1,
        runnable -> {
          Thread thread = new Thread(runnable, "hop-notifications");
          thread.setDaemon(true);
          return thread;
        });
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
    String message = describe(e);
    boolean hadError = providerErrors.containsKey(providerId);
    providerErrors.put(
        providerId, new ProviderErrorInfo(providerId, providerName, message, new Date()));
    if (!hadError) {
      notifyListeners();
    }
  }

  /**
   * A one-line description of a failure, for the error banner. {@link
   * org.apache.hop.core.exception.HopException#getMessage()} folds in the cause across several
   * lines, which a single-line label would render unreadably.
   *
   * @param e The failure to describe
   * @return A trimmed, single-line message
   */
  private static String describe(Throwable e) {
    String message = e.getMessage();
    if (message == null || message.isBlank()) {
      message = e.getClass().getSimpleName();
    }
    message = message.replaceAll("\\s+", " ").trim();
    if (message.isEmpty()) {
      message = e.getClass().getSimpleName();
    }
    if (message.length() > MAX_ERROR_MESSAGE_LENGTH) {
      message = message.substring(0, MAX_ERROR_MESSAGE_LENGTH - 3) + "...";
    }
    return message;
  }

  private void clearProviderError(String providerId) {
    if (providerErrors.remove(providerId) != null) {
      notifyListeners();
    }
  }

  /** Start the notification service */
  public void start() {
    log.logDetailed("Starting notification service");
    stopping = false;

    // Initialize scheduler
    if (scheduler == null || scheduler.isShutdown()) {
      scheduler = newScheduler();
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
              if (provider.isEnabled()) {
                fetchFrom(provider);
              }
            },
            INITIAL_FETCH_DELAY_MS, // Initial delay: the first fetch, off the UI thread
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
    stopping = true;

    // Cancel all scheduled tasks
    for (ScheduledFuture<?> task : scheduledTasks.values()) {
      if (task != null) {
        task.cancel(false);
      }
    }
    scheduledTasks.clear();

    // Shut the scheduler down without waiting for it. This is called from the UI thread when the
    // user switches notifications off, and a poll in flight is talking to a source that has ten
    // seconds to answer the connection and twenty to answer the request: waiting for it here is
    // waiting with the whole GUI frozen. The threads are daemons and are interrupted here, so
    // nothing is left holding the process open.
    if (scheduler != null && !scheduler.isShutdown()) {
      scheduler.shutdownNow();
    }

    // Shutdown providers
    for (INotificationProvider provider : providers.values()) {
      try {
        provider.shutdown();
      } catch (Exception e) {
        log.logError("Error shutting down provider: " + provider.getName(), e);
      }
    }
    // Listeners are deliberately kept. They belong to the badge and the panel, which register
    // once when they are created and live as long as the window does. Dropping them here left
    // them silently detached after the user switched notifications off and on again: polling
    // resumed, but there was no longer anybody to tell.
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
          // Presence means read. Older state stored the flag "true" rather than a moment; treat
          // that as read just now, so it is the last to be pruned rather than the first.
          if (!"false".equalsIgnoreCase(entry.getValue())) {
            persistedReadState.put(entry.getKey(), toMoment(entry.getValue()));
          }
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
      prune(persistedReadState, "read notification(s)");
      Map<String, String> readStateMap = new java.util.HashMap<>();
      for (Map.Entry<String, Long> entry : persistedReadState.entrySet()) {
        readStateMap.put(entry.getKey(), String.valueOf(entry.getValue()));
      }
      auditManager.saveMap(namespace, AUDIT_TYPE_READ_STATE, readStateMap);
      log.logDetailed("Saved persisted read state for " + readStateMap.size() + " notification(s)");
    } catch (Exception e) {
      log.logError("Error saving persisted notification read state", e);
    }
  }

  /**
   * Read a stored moment, tolerating the flags that earlier versions wrote there.
   *
   * @param value The stored value
   * @return The moment it records, or now when it records none
   */
  private static long toMoment(String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (RuntimeException e) {
      return System.currentTimeMillis();
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
        for (Map.Entry<String, String> entry : removedMap.entrySet()) {
          persistedRemovedIds.put(entry.getKey(), toMoment(entry.getValue()));
        }
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
      prune(persistedRemovedIds, "removed notification(s)");
      Map<String, String> removedMap = new java.util.HashMap<>();
      for (Map.Entry<String, Long> entry : persistedRemovedIds.entrySet()) {
        removedMap.put(entry.getKey(), String.valueOf(entry.getValue()));
      }
      auditManager.saveMap(namespace, AUDIT_TYPE_REMOVED_IDS, removedMap);
      log.logDetailed(
          "Saved persisted removed state for " + removedMap.size() + " notification(s)");
    } catch (Exception e) {
      log.logError("Error saving persisted notification removed state", e);
    }
  }
}
