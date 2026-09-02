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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.notifications.providers.TestNotificationProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for NotificationService. */
public class NotificationServiceTest {

  private NotificationService service;

  @BeforeAll
  public static void initHopLogStore() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @BeforeEach
  public void setUp() {
    clearPersistedNotificationState();
    // Constructed rather than fetched through getInstance(): that resolves the per-process or
    // per-session provider, which needs the desktop or web module on the classpath, and a test
    // wants its own service anyway.
    service = new NotificationService();
  }

  @AfterEach
  public void tearDown() {
    if (service != null) {
      service.stop();
    }
  }

  /**
   * Read and removed state is persisted through the audit manager, which writes to a real folder
   * that outlives the JVM. Without clearing it the suite passes once and then fails on every later
   * run against the same target directory, because the notifications come back already read.
   */
  private void clearPersistedNotificationState() {
    IAuditManager auditManager = AuditManager.getActive();
    if (auditManager == null) {
      return;
    }
    try {
      auditManager.saveMap(
          HopGui.DEFAULT_HOP_GUI_NAMESPACE, "notifications-read-state", new HashMap<>());
      auditManager.saveMap(
          HopGui.DEFAULT_HOP_GUI_NAMESPACE, "notifications-removed-ids", new HashMap<>());
    } catch (Exception e) {
      // Best effort: a missing audit manager only means there was nothing to carry over.
    }
  }

  @Test
  public void testRegisterAndUnregisterProvider() {
    TestNotificationProvider provider = new TestNotificationProvider();
    service.registerProvider(provider);

    assertNotNull(service.getProvider("test-provider"));
    assertEquals("Test Notification Provider", service.getProvider("test-provider").getName());

    service.unregisterProvider("test-provider");
    assertNull(service.getProvider("test-provider"));
  }

  @Test
  public void testRegisterNullProviderIsIgnored() {
    service.registerProvider(null);
    assertEquals(0, service.getTotalCount());
  }

  @Test
  public void testAddNotification() {
    Notification n =
        new Notification(
            "notif-1",
            "Test",
            "Message",
            "Source",
            "source-id",
            null,
            new Date(),
            NotificationPriority.INFO,
            NotificationCategory.OTHER);

    service.addNotification(n);

    List<Notification> all = service.getNotifications(false);
    assertEquals(1, all.size());
    assertEquals("source-id:notif-1", all.get(0).getId());
    assertEquals("Test", all.get(0).getTitle());
  }

  @Test
  public void testAddNotification_duplicateIgnored() {
    Notification n =
        new Notification(
            "notif-1",
            "Test",
            "Message",
            "Source",
            "source-id",
            null,
            new Date(),
            NotificationPriority.INFO,
            NotificationCategory.OTHER);

    service.addNotification(n);
    service.addNotification(n);

    assertEquals(1, service.getTotalCount());
  }

  @Test
  public void testMarkAsRead() {
    Notification n =
        new Notification(
            "notif-1",
            "Test",
            "Message",
            "Source",
            "source-id",
            null,
            new Date(),
            NotificationPriority.INFO,
            NotificationCategory.OTHER);
    service.addNotification(n);

    assertEquals(1, service.getUnreadCount());
    service.markAsRead("source-id:notif-1");
    assertEquals(0, service.getUnreadCount());

    List<Notification> unreadOnly = service.getNotifications(true);
    assertTrue(unreadOnly.isEmpty());
  }

  @Test
  public void testGetNotifications_unreadOnly() {
    Notification n1 =
        new Notification(
            "notif-1",
            "Unread",
            "Msg",
            "Source",
            "sid",
            null,
            new Date(),
            NotificationPriority.INFO,
            NotificationCategory.OTHER);
    Notification n2 =
        new Notification(
            "notif-2",
            "Read",
            "Msg",
            "Source",
            "sid",
            null,
            new Date(),
            NotificationPriority.INFO,
            NotificationCategory.OTHER);
    n2.setRead(true);

    service.addNotification(n1);
    service.addNotification(n2);

    List<Notification> unread = service.getNotifications(true);
    assertEquals(1, unread.size());
    assertEquals("sid:notif-1", unread.get(0).getId());
  }

  @Test
  public void testRemoveNotification() {
    Notification n =
        new Notification(
            "notif-1",
            "Test",
            "Message",
            "Source",
            "source-id",
            null,
            new Date(),
            NotificationPriority.INFO,
            NotificationCategory.OTHER);
    service.addNotification(n);
    assertEquals(1, service.getTotalCount());

    service.removeNotification("source-id:notif-1");
    assertEquals(0, service.getTotalCount());
  }

  @Test
  public void testFetchFromProviders() throws Exception {
    TestNotificationProvider provider = new TestNotificationProvider();
    service.registerProvider(provider);

    service.fetchFromProviders();

    assertTrue(service.getTotalCount() >= 1);
  }

  @Test
  public void testUnsafeLinkIsDropped() {
    Notification n = notificationWithLink("notif-unsafe", "file:///etc/passwd");

    service.addNotification(n);

    assertEquals(1, service.getTotalCount());
    assertNull(service.getNotifications(false).get(0).getLink());
  }

  @Test
  public void testSafeLinkIsKept() {
    Notification n = notificationWithLink("notif-safe", "https://hop.apache.org/download");

    service.addNotification(n);

    assertEquals(
        "https://hop.apache.org/download", service.getNotifications(false).get(0).getLink());
  }

  @Test
  public void testFailingProviderIsReportedAsProviderError() throws Exception {
    service.registerProvider(new FailingProvider());

    service.fetchFromProviders();

    List<ProviderErrorInfo> errors = service.getProviderErrors();
    assertEquals(1, errors.size());
    ProviderErrorInfo error = errors.get(0);
    assertEquals("Failing Provider", error.getProviderName());
    assertTrue(error.getMessage().contains("the feed is unreachable"));
    // The banner is a single-line label, and HopException folds the cause in across lines.
    assertFalse(error.getMessage().contains("\n"));
  }

  @Test
  public void testProviderErrorIsClearedOnSuccess() throws Exception {
    FailingProvider provider = new FailingProvider();
    service.registerProvider(provider);
    service.fetchFromProviders();
    assertEquals(1, service.getProviderErrors().size());

    provider.failing = false;
    service.fetchFromProviders();

    assertTrue(service.getProviderErrors().isEmpty());
  }

  @Test
  public void testRetryNowDoesNotBlockAndNotifiesListeners() throws Exception {
    CountDownLatch notified = new CountDownLatch(1);
    service.addNotificationListener(notified::countDown);
    service.registerProvider(new TestNotificationProvider());

    service.retryNow();

    assertTrue(notified.await(10, TimeUnit.SECONDS), "retryNow() never notified its listeners");
  }

  @Test
  public void testIdIsQualifiedWithTheSource() {
    Notification n = notificationFrom("2.19.0", "github-apache-hop");

    service.addNotification(n);

    assertEquals("github-apache-hop:2.19.0", service.getNotifications(false).get(0).getId());
  }

  @Test
  public void testSameIdFromDifferentSourcesAreDistinctNotifications() {
    // Two repositories releasing the same version number. Before the id carried its source these
    // collapsed into one notification, and marking one read marked the other read too.
    service.addNotification(notificationFrom("2.19.0", "github-apache-hop"));
    service.addNotification(notificationFrom("2.19.0", "github-example-fork"));

    assertEquals(2, service.getTotalCount());
    assertEquals(2, service.getUnreadCount());

    service.markAsRead("github-apache-hop:2.19.0");

    assertEquals(1, service.getUnreadCount());
    assertEquals("github-example-fork:2.19.0", service.getNotifications(true).get(0).getId());
  }

  @Test
  public void testQualifyingIsIdempotent() {
    // A provider handing back the same instances on a later poll must not accumulate prefixes.
    Notification n = notificationFrom("2.19.0", "github-apache-hop");

    service.addNotification(n);
    service.addNotification(n);

    assertEquals(1, service.getTotalCount());
    assertEquals("github-apache-hop:2.19.0", service.getNotifications(false).get(0).getId());
  }

  @Test
  public void testNotificationWithoutSourceKeepsItsOwnId() {
    Notification n = notificationFrom("orphan", null);

    service.addNotification(n);

    assertEquals("orphan", service.getNotifications(false).get(0).getId());
  }

  @Test
  public void testRetainedNotificationsAreCappedDroppingTheOldest() {
    // A busy feed would otherwise grow the list for as long as Hop is running, and every entry
    // becomes a stack of widgets when the panel opens.
    for (int i = 0; i < 520; i++) {
      Notification n = notificationFrom("n-" + i, "sid");
      n.setTimestamp(new Date(1_000_000L + i));
      service.addNotification(n);
    }

    assertEquals(500, service.getTotalCount());
    List<Notification> kept = service.getNotifications(false);
    // Sorted newest first, so the newest survived and the oldest twenty went.
    assertEquals("sid:n-519", kept.get(0).getId());
    assertEquals("sid:n-20", kept.get(kept.size() - 1).getId());
  }

  @Test
  public void testReadStateSurvivesTheNotificationBeingDropped() {
    Notification n = notificationFrom("n-read", "sid");
    n.setTimestamp(new Date(1L));
    service.addNotification(n);
    service.markAsRead("sid:n-read");
    service.removeNotification("sid:n-read");

    // The source offers it again on the next poll; it must not come back unread.
    Notification again = notificationFrom("n-read", "sid");
    again.setTimestamp(new Date(1L));
    service.addNotification(again);

    assertEquals(1, service.getTotalCount());
    assertEquals(0, service.getUnreadCount());
  }

  @Test
  public void testUnregisteringASourceRemovesItsNotifications() {
    // Deleting a source used to leave its notifications in the panel for good: the provider went,
    // what it had contributed stayed.
    service.registerProvider(new TestNotificationProvider());
    service.addNotification(notificationFrom("keep-me", "another-source"));
    service.addNotification(notificationFrom("drop-me", "test-provider"));
    assertEquals(2, service.getTotalCount());

    service.unregisterProvider("test-provider");

    assertEquals(1, service.getTotalCount());
    assertEquals("another-source:keep-me", service.getNotifications(false).get(0).getId());
  }

  @Test
  public void testUnregisteringKeepsReadStateSoAReturningSourceIsNotAllNew() {
    service.registerProvider(new TestNotificationProvider());
    service.addNotification(notificationFrom("seen", "test-provider"));
    service.markAsRead("test-provider:seen");
    service.unregisterProvider("test-provider");
    assertEquals(0, service.getTotalCount());

    // The source comes back and offers the same notification again.
    service.addNotification(notificationFrom("seen", "test-provider"));

    assertEquals(1, service.getTotalCount());
    assertEquals(0, service.getUnreadCount());
  }

  @Test
  public void testUnregisteringAnUnknownProviderChangesNothing() {
    service.addNotification(notificationFrom("keep-me", "a-source"));

    service.unregisterProvider("never-registered");

    assertEquals(1, service.getTotalCount());
  }

  @Test
  public void testListenersSurviveTheServiceBeingStoppedAndStartedAgain() {
    // Switching notifications off in the settings stops the service; switching them back on
    // starts it again. The badge and the panel register once, when they are created, so a stop
    // that forgot them left them silently detached for the rest of the session.
    int[] changes = {0};
    service.addNotificationListener(() -> changes[0]++);

    service.stop();
    service.start();
    service.addNotification(notificationFrom("after-restart", "a-source"));

    assertEquals(1, changes[0]);
  }

  private Notification notificationFrom(String id, String sourceId) {
    Notification n = new Notification();
    n.setId(id);
    n.setTitle("Title");
    n.setMessage("Message");
    n.setSource("test");
    n.setSourceId(sourceId);
    n.setTimestamp(new Date());
    return n;
  }

  private Notification notificationWithLink(String id, String link) {
    Notification n = new Notification();
    n.setId(id);
    n.setTitle("Title");
    n.setMessage("Message");
    n.setSource("test");
    n.setSourceId("test-provider");
    n.setLink(link);
    n.setTimestamp(new Date());
    return n;
  }

  /** A provider that fails the way an unreachable feed does. */
  private static class FailingProvider implements INotificationProvider {
    private boolean failing = true;

    @Override
    public String getId() {
      return "failing-provider";
    }

    @Override
    public String getName() {
      return "Failing Provider";
    }

    @Override
    public String getDescription() {
      return "Always fails";
    }

    @Override
    public List<Notification> fetchNotifications() throws HopException {
      if (failing) {
        throw new HopException(
            "the feed is unreachable", new java.net.UnknownHostException("nope"));
      }
      return new java.util.ArrayList<>();
    }

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public void setEnabled(boolean enabled) {
      // Always enabled
    }

    @Override
    public long getPollInterval() {
      return 3600000;
    }

    @Override
    public void setPollInterval(long interval) {
      // Fixed
    }

    @Override
    public void initialize() {
      // Nothing to initialize
    }

    @Override
    public void shutdown() {
      // Nothing to clean up
    }
  }
}
