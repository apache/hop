/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.util.LinkedHashMap;
import java.util.Map;

/** One candidate per URI. Repeated hints do not restart an unchanged candidate's quiet interval. */
public class FileStabilityTracker {
  private final Map<String, Candidate> candidates = new LinkedHashMap<>();
  private final boolean waitUntilStable;
  private final long minimumAge;
  private final int checks;
  private final long interval;
  private final int capacity;

  public FileStabilityTracker(
      boolean waitUntilStable, long minimumAge, int checks, long interval, int capacity) {
    this.waitUntilStable = waitUntilStable;
    this.minimumAge = minimumAge;
    this.checks = checks;
    this.interval = interval;
    this.capacity = capacity;
  }

  public void observe(FileChangeEvent event, long now) {
    String uri = event.file().getUri();
    if (event.getType() == FileEventType.DELETED) {
      candidates.remove(uri);
      return;
    }
    Candidate existing = candidates.get(uri);
    if (existing != null && event.getCurrent().sameVersion(existing.event.getCurrent())) {
      return;
    }
    if (existing == null && candidates.size() >= capacity) {
      throw new IllegalStateException("Pending file limit exceeded. Increase maximum entries.");
    }
    // Previous always comes from the committed observed state, not another native hint.
    candidates.put(uri, new Candidate(event, now));
  }

  public boolean due(String uri, long now) {
    return due(uri, now, now);
  }

  public boolean due(String uri, long now, long wallTime) {
    Candidate candidate = candidates.get(uri);
    return candidate != null
        && wallTime - candidate.event.getCurrent().getLastModified() >= minimumAge
        && (!waitUntilStable || now - candidate.checkedAt >= interval);
  }

  public FileChangeEvent check(String uri, FileState current, long now) {
    return check(uri, current, now, now);
  }

  public FileChangeEvent check(String uri, FileState current, long now, long wallTime) {
    Candidate candidate = candidates.get(uri);
    if (candidate == null) {
      return null;
    }
    if (current == null) {
      candidates.remove(uri);
      return null;
    }
    if (!current.sameVersion(candidate.event.getCurrent())) {
      candidate.event =
          new FileChangeEvent(
              candidate.event.getType(), current, candidate.event.getPrevious(), wallTime);
      candidate.checkedAt = now;
      candidate.equalChecks = 0;
      return null;
    }
    if (now - candidate.checkedAt >= interval) {
      candidate.checkedAt = now;
      candidate.equalChecks++;
    }
    if (wallTime - current.getLastModified() < minimumAge
        || (waitUntilStable && candidate.equalChecks < checks)) {
      return null;
    }
    return new FileChangeEvent(
        candidate.event.getType(), current, candidate.event.getPrevious(), wallTime);
  }

  public String[] uris() {
    return candidates.keySet().toArray(String[]::new);
  }

  public int size() {
    return candidates.size();
  }

  /** Rotate candidates in O(1), without copying the whole tree for every emitted row. */
  public String nextUri() {
    if (candidates.isEmpty()) {
      return null;
    }
    String uri = candidates.keySet().iterator().next();
    Candidate candidate = candidates.remove(uri);
    candidates.put(uri, candidate);
    return uri;
  }

  public void remove(String uri) {
    candidates.remove(uri);
  }

  private static class Candidate {
    private FileChangeEvent event;
    private long checkedAt;
    private int equalChecks;

    private Candidate(FileChangeEvent event, long now) {
      this.event = event;
      this.checkedAt = now;
    }
  }
}
