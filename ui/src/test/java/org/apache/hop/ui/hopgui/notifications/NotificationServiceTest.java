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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;
import org.apache.hop.ui.hopgui.notifications.providers.TestNotificationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for NotificationService. */
public class NotificationServiceTest {

  private NotificationService service;

  @BeforeClass
  public static void initHopLogStore() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Before
  public void setUp() throws Exception {
    resetNotificationServiceSingleton();
    service = NotificationService.getInstance();
  }

  @After
  public void tearDown() throws Exception {
    if (service != null) {
      service.stop();
    }
    resetNotificationServiceSingleton();
  }

  private void resetNotificationServiceSingleton() throws Exception {
    Field instanceField = NotificationService.class.getDeclaredField("instance");
    instanceField.setAccessible(true);
    instanceField.set(null, null);
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
    assertEquals("notif-1", all.get(0).getId());
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
    service.markAsRead("notif-1");
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
    assertEquals("notif-1", unread.get(0).getId());
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

    service.removeNotification("notif-1");
    assertEquals(0, service.getTotalCount());
  }

  @Test
  public void testFetchFromProviders() throws Exception {
    TestNotificationProvider provider = new TestNotificationProvider();
    service.registerProvider(provider);

    service.fetchFromProviders();

    assertTrue(service.getTotalCount() >= 1);
  }
}
