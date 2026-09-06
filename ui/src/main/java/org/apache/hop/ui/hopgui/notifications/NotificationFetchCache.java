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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.notifications.INotificationProvider;
import org.apache.hop.core.notifications.Notification;

/**
 * One poll of a source per process, however many sessions want it.
 *
 * <p>{@link NotificationService} is per user session in Hop Web, because read state, removals and
 * listeners belong to one user. The sources it polls do not: they live in the process-wide {@code
 * hop-config.json}, so every session polls the same list. Ten sessions signing in after a restart
 * therefore make ten identical calls to the same source within seconds of each other.
 *
 * <p>That is mostly a burst rather than a sustained cost, because the providers make conditional
 * requests and GitHub does not count a 304 against the rate limit. But the burst is real, the
 * default source is unauthenticated {@code api.github.com} at sixty requests an hour per IP
 * address, and a source that is down is a burst of failures rather than a burst of successes.
 *
 * <p>So a fetch is shared: the first caller does the work, callers arriving while it is in flight
 * wait for it rather than starting their own, and callers arriving shortly after get what it
 * returned. Each caller is given copies, since the notifications it is handed are marked read as
 * that session's user reads them.
 *
 * <p>Only answers are shared, not failures. A source that has just failed is asked again by the
 * next caller: holding on to a failure would keep a source that has recovered looking broken to
 * every session, and the burst this exists for is simultaneous, so waiting on the in-flight fetch
 * already collapses it. Nothing here is persisted; a restart starts over.
 */
public final class NotificationFetchCache {

  /**
   * How long an answer stands in for the next poll, as a fraction of the poll interval. Half an
   * interval is short enough that a source is never polled at appreciably less than the interval
   * the user asked for, and long enough to collapse the sign-in burst.
   */
  private static final int FRESHNESS_DIVISOR = 2;

  /** Below this, a source is being polled fast enough that sharing is all that is wanted. */
  private static final long MINIMUM_FRESHNESS_MS = 60000;

  private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

  private NotificationFetchCache() {
    // Utility class
  }

  /**
   * Poll a source, or reuse what an equivalent poll just returned.
   *
   * @param provider The provider to poll
   * @return What the source is offering, as copies belonging to the caller
   * @throws HopException Whatever the provider threw
   */
  public static List<Notification> fetch(INotificationProvider provider) throws HopException {
    String key = provider.getId();
    if (key == null) {
      // Nothing to share it under. NotificationService logs the source that has no id.
      return provider.fetchNotifications();
    }

    long freshFor = freshnessOf(provider);
    Entry entry = ENTRIES.computeIfAbsent(key, k -> new Entry());
    synchronized (entry) {
      // Checked again inside the lock: the caller that held it may have been fetching this very
      // source, which is the case this exists for.
      List<Notification> fresh = entry.answer(freshFor);
      if (fresh != null) {
        return fresh;
      }
      List<Notification> fetched = provider.fetchNotifications();
      entry.remember(fetched);
      return entry.copies();
    }
  }

  /**
   * Forget what a source last answered, so the next poll really polls.
   *
   * <p>The Retry button means "ask again", not "show me the failure again", and a source that has
   * been reconfigured has to be re-read whatever it said a minute ago.
   *
   * @param sourceId The source to forget, or null to forget all of them
   */
  public static void invalidate(String sourceId) {
    if (sourceId == null) {
      ENTRIES.clear();
    } else {
      ENTRIES.remove(sourceId);
    }
  }

  private static long freshnessOf(INotificationProvider provider) {
    long pollInterval = provider.getPollInterval();
    if (pollInterval <= 0) {
      pollInterval = 3600000;
    }
    return Math.max(pollInterval / FRESHNESS_DIVISOR, MINIMUM_FRESHNESS_MS);
  }

  private static final class Entry {
    private long fetchedAt = Long.MIN_VALUE;
    private List<Notification> result;

    /**
     * @return Copies of the last answer while it is still fresh, or null when a poll is due
     */
    private List<Notification> answer(long freshFor) {
      if (result == null || System.currentTimeMillis() - fetchedAt >= freshFor) {
        return null;
      }
      return copies();
    }

    private List<Notification> copies() {
      List<Notification> copies = new ArrayList<>(result.size());
      for (Notification notification : result) {
        copies.add(new Notification(notification));
      }
      return copies;
    }

    private void remember(List<Notification> fetched) {
      this.result = fetched == null ? new ArrayList<>() : new ArrayList<>(fetched);
      this.fetchedAt = System.currentTimeMillis();
    }
  }
}
