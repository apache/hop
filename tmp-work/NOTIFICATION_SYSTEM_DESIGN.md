# Notification System Design

## Overview

The Apache Hop notification system displays release announcements, plugin updates, and other feeds in the Hop GUI. Users access notifications via the bell icon in the toolbar; a dropdown panel shows items from configured sources (GitHub releases, RSS/Atom feeds, custom plugin sources).

## Architecture

### Components

| Component | Location | Purpose |
|-----------|----------|---------|
| **NotificationService** | `org.apache.hop.ui.hopgui.notifications` | Central service: manages providers, polling, notifications list, read state |
| **NotificationPanel** | `org.apache.hop.ui.hopgui.notifications` | UI: dropdown panel, notification items, error banner |
| **NotificationToolbarItem** | `org.apache.hop.ui.hopgui.notifications` | Toolbar bell icon and badge |
| **NotificationConfigPlugin** | `org.apache.hop.ui.hopgui.notifications.config` | Configuration perspective tab for sources |
| **NotificationProviderFactory** | `org.apache.hop.ui.hopgui.notifications` | Creates `INotificationProvider` from config |
| **NotificationSystemInitializer** | `org.apache.hop.ui.hopgui.notifications` | Extension point: starts notification system on Hop GUI startup |

### Data Flow

1. **Startup**: `NotificationSystemInitializer` loads sources from `HopConfig`, creates providers via `NotificationProviderFactory`, registers them with `NotificationService`, and starts polling.
2. **Polling**: `NotificationService` runs `fetchFromProviders()` on a schedule per provider. Errors are recorded via `recordProviderError()`; success clears the error via `clearProviderError()`.
3. **UI**: `NotificationPanel` displays provider errors (banner with Retry) and notifications. Users can mark as read, clear all, open links, or configure sources via the Settings button.

### Configuration (HopConfig)

- `notification.system.enabled` – Enable/disable the notification system (default: `true`)
- `notification.sources` – JSON array of `NotificationSourceConfig`
- `notification.showReadNotifications` – Show read notifications (default: `true`)
- `notification.global.daysToGoBack` – Limit age of notifications in days (default: `30`)

---

## Error Handling and Offline Behavior

### Provider Error Tracking

- When a provider throws during `fetchNotifications()` or polling, the exception is caught and logged.
- `NotificationService.recordProviderError(providerId, providerName, throwable)` stores the last error per provider.
- On success, `clearProviderError(providerId)` removes the error and listeners are notified.
- There is no automatic retry; polling continues on the normal schedule.

### User-Facing Error UI

- `NotificationPanel` calls `NotificationService.getProviderErrors()` and displays an error banner when non-empty.
- The banner shows: "Some notification sources could not be loaded" plus each provider name and message (e.g., `Apache Hop Releases: Connection refused`).
- A **Retry** button calls `NotificationService.retryNow()`, which triggers an immediate fetch from all providers and refreshes the panel.

### Offline / Network Failures

- Offline or network failures are treated like any other exception: logged and surfaced in the error banner.
- Users can use Retry when connectivity is restored.

---

## User Guide

### Using the Notification Panel

1. **Open notifications**: Click the bell icon in the main toolbar.
2. **View items**: Each item shows title, message, timestamp, and source color.
3. **Mark as read**: Click a notification to mark it read (gray background).
4. **Open link**: Click the external link icon or the notification to open the URL.
5. **Clear all**: Use "Clear all" to mark everything as read and remove items.
6. **Settings**: Click the gear icon to open the Notifications configuration tab.

### When Sources Fail

If one or more sources fail (e.g., network error, invalid URL):

- A yellow banner appears at the top of the panel: "Some notification sources could not be loaded".
- Each failed source is listed with its error message.
- Click **Retry** to try again immediately. If the issue is fixed (e.g., network restored), the banner disappears and notifications load as usual.

### Configuring Notification Sources

1. Go to **File → Configuration**.
2. Select the **Notifications** tab.
3. Add, edit, or remove sources. Supported types:
   - **GitHub Releases** – Repository owner/repo (e.g., apache/hop)
   - **RSS/Atom** – Feed URL
   - **Custom Plugin** – Provided by a plugin; configure poll interval and color only.
4. Save. Changes take effect immediately; the notification panel will update on next poll or when you click Retry.

---

## Plugin Developer Guide

### Implementing a Notification Provider

1. Implement `INotificationProvider`:

```java
public class MyPluginNotificationProvider implements INotificationProvider {
  private String id = "my-plugin-notifications";
  private boolean enabled = true;
  private long pollInterval = 3600000;

  @Override
  public List<Notification> fetchNotifications() throws HopException {
    List<Notification> notifications = new ArrayList<>();
    Notification notif = new Notification();
    notif.setId("my-notif-1");
    notif.setTitle("Plugin Update Available");
    notif.setMessage("Version 2.0 is now available!");
    notif.setSource("my-plugin");
    notif.setSourceId("my-plugin-notifications"); // Must match provider.getId()
    notif.setLink("https://example.com/plugin/download");
    notif.setTimestamp(new Date());
    notifications.add(notif);
    return notifications;
  }
  // ... getId(), getName(), getDescription(), isEnabled(), setEnabled(), getPollInterval(), setPollInterval(), initialize(), shutdown()
}
```

2. Register via `NotificationPluginHelper` during plugin initialization:

```java
@GuiPlugin(description = "My Plugin")
public class MyPluginGuiPlugin {
  public MyPluginGuiPlugin() {
    INotificationProvider provider = new MyPluginNotificationProvider();
    NotificationPluginHelper.registerPluginNotifications(
        "my-plugin-notifications",   // Plugin ID (must match provider.getId())
        "My Plugin Notifications",   // Display name
        provider,
        60,                          // Poll interval in minutes (optional)
        "#FF5733"                    // Color hex (optional)
    );
  }
}
```

### Error Handling in Providers

- If `fetchNotifications()` throws, the exception is caught by `NotificationService`, logged, and recorded as a provider error.
- Users see the error in the panel and can use Retry. No extra handling is required in the provider.

### API Reference

- **INotificationProvider** – `org.apache.hop.core.notifications.INotificationProvider`
- **NotificationPluginHelper** – `org.apache.hop.ui.hopgui.notifications.NotificationPluginHelper`
- **Notification** – `org.apache.hop.core.notifications.Notification`

See Javadoc on these classes for full details.

---

## Configuration Reference

### NotificationSourceConfig (per source)

| Field | Type | Description |
|-------|------|-------------|
| `id` | String | Unique ID (e.g., `github-apache-hop`) |
| `name` | String | Display name |
| `type` | enum | `GITHUB_RELEASES`, `RSS`, `CUSTOM_PLUGIN` |
| `enabled` | boolean | Whether to poll this source |
| `pollIntervalMinutes` | String | Poll interval in minutes |
| `color` | String | Hex color (e.g., `#FF5733`) |
| GitHub | `githubOwner`, `githubRepo`, `githubIncludePrereleases` | For `GITHUB_RELEASES` |
| RSS | `rssUrl` | For `RSS` |
| Custom | `pluginId` | For `CUSTOM_PLUGIN`; falls back to `id` |

---

## Hot Reload

When the user saves notification settings in the Configuration perspective:

1. `NotificationConfigPlugin` calls `ConfigurationPerspective.notifyConfigChanged("notification.sources")`.
2. `NotificationService.reloadFromConfig()` syncs providers with config: unregisters removed/disabled, registers new ones, updates CUSTOM_PLUGIN poll intervals.
3. Polling continues; no restart needed.
