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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;
import org.apache.hop.core.notifications.NotificationCategory;
import org.apache.hop.core.notifications.NotificationPriority;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * One poll of a source per process, however many Hop Web sessions want it.
 *
 * <p>Each session has its own {@link NotificationService} because read state belongs to one user,
 * but the sources they poll are the process-wide ones in hop-config.json. Without this the sessions
 * signing in after a restart make one identical call each to the same source.
 */
public class NotificationFetchCacheTest {

  @BeforeEach
  public void setUp() {
    NotificationFetchCache.invalidate(null);
  }

  @Test
  public void testASecondCallerGetsTheFirstCallersAnswer() throws Exception {
    CountingProvider provider = new CountingProvider("shared-source");

    NotificationFetchCache.fetch(provider);
    NotificationFetchCache.fetch(provider);
    NotificationFetchCache.fetch(provider);

    assertEquals(1, provider.calls.get());
  }

  @Test
  public void testEachCallerGetsItsOwnCopies() throws Exception {
    // Read state is set on the notification itself, so sharing the objects would mean one user
    // reading an item marked it read for every other session.
    CountingProvider provider = new CountingProvider("shared-source");

    Notification first = NotificationFetchCache.fetch(provider).get(0);
    Notification second = NotificationFetchCache.fetch(provider).get(0);

    assertNotSame(first, second);
    assertEquals(first.getId(), second.getId());
    first.setRead(true);
    assertTrue(!second.isRead());
  }

  @Test
  public void testADifferentSourceIsPolledSeparately() throws Exception {
    CountingProvider one = new CountingProvider("source-one");
    CountingProvider two = new CountingProvider("source-two");

    NotificationFetchCache.fetch(one);
    NotificationFetchCache.fetch(two);

    assertEquals(1, one.calls.get());
    assertEquals(1, two.calls.get());
  }

  @Test
  public void testInvalidatingMakesTheNextCallerPollAgain() throws Exception {
    // What the Retry button and a save of the settings both rely on.
    CountingProvider provider = new CountingProvider("shared-source");

    NotificationFetchCache.fetch(provider);
    NotificationFetchCache.invalidate("shared-source");
    NotificationFetchCache.fetch(provider);

    assertEquals(2, provider.calls.get());
  }

  @Test
  public void testAFailureIsNotHeldOnTo() throws Exception {
    // A source that has recovered must not go on looking broken to every session.
    FailingOnceProvider provider = new FailingOnceProvider();

    assertThrows(HopException.class, () -> NotificationFetchCache.fetch(provider));
    List<Notification> recovered = NotificationFetchCache.fetch(provider);

    assertEquals(1, recovered.size());
  }

  @Test
  public void testCallersArrivingTogetherWaitForOneFetch() throws Exception {
    // The case this exists for: sessions signing in at the same time after a restart.
    CountingProvider provider = new CountingProvider("shared-source");
    provider.entered = new CountDownLatch(1);
    provider.release = new CountDownLatch(1);

    int callers = 5;
    CountDownLatch done = new CountDownLatch(callers);
    for (int i = 0; i < callers; i++) {
      Thread thread =
          new Thread(
              () -> {
                try {
                  NotificationFetchCache.fetch(provider);
                } catch (Exception e) {
                  // Counted below as a missing completion.
                  return;
                }
                done.countDown();
              });
      thread.setDaemon(true);
      thread.start();
    }

    assertTrue(provider.entered.await(10, TimeUnit.SECONDS), "no caller reached the provider");
    provider.release.countDown();
    assertTrue(done.await(10, TimeUnit.SECONDS), "callers did not all finish");
    assertEquals(1, provider.calls.get());
  }

  private static Notification notification(String sourceId) {
    return new Notification(
        "notif-1",
        "A release",
        "Something happened",
        "test",
        sourceId,
        null,
        new Date(),
        NotificationPriority.INFO,
        NotificationCategory.OTHER);
  }

  /** Counts how often it is actually asked. */
  private static class CountingProvider implements INotificationProvider {
    private final String id;
    private final AtomicInteger calls = new AtomicInteger();
    private CountDownLatch entered;
    private CountDownLatch release;

    private CountingProvider(String id) {
      this.id = id;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getName() {
      return "Counting Provider";
    }

    @Override
    public String getDescription() {
      return "Counts how often it is asked";
    }

    @Override
    public List<Notification> fetchNotifications() throws HopException {
      calls.incrementAndGet();
      if (entered != null) {
        entered.countDown();
        try {
          release.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      List<Notification> notifications = new ArrayList<>();
      notifications.add(notification(id));
      return notifications;
    }

    @Override
    public void initialize() {
      // Nothing to do
    }

    @Override
    public void shutdown() {
      // Nothing to do
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
  }

  /** Fails the first time and succeeds afterwards. */
  private static class FailingOnceProvider extends CountingProvider {
    private boolean failed;

    private FailingOnceProvider() {
      super("recovering-source");
    }

    @Override
    public List<Notification> fetchNotifications() throws HopException {
      if (!failed) {
        failed = true;
        throw new HopException("the feed is unreachable");
      }
      return super.fetchNotifications();
    }
  }
}
