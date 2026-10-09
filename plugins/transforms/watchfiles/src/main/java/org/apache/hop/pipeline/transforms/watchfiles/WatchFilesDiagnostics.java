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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.Getter;

/** Counters have one writer; readers see an immutable, volatile snapshot. No monitoring thread. */
public class WatchFilesDiagnostics {
  private static final ConcurrentHashMap<String, WatchFilesDiagnostics> ACTIVE =
      new ConcurrentHashMap<>();
  private final String key;
  private final String watchId;
  private final WatchFilesClock clock;
  private final Consumer<String> log;
  private final long slowMillis;
  private final long reportInterval;
  private long nextReport;
  private long scans;
  private long checkpoints;
  private long retries;
  private long overflow;
  private long emitted;
  private long suppressed;
  private long lastScanAt;
  private long scanMillis;
  private long checkpointMillis;
  private String lastFailure = "";
  @Getter private volatile Snapshot snapshot;

  public WatchFilesDiagnostics(
      String key,
      String watchId,
      WatchFilesClock clock,
      long slowMillis,
      long reportInterval,
      Consumer<String> log) {
    this.key = key;
    this.watchId = watchId;
    this.clock = clock;
    this.slowMillis = slowMillis;
    this.reportInterval = reportInterval;
    this.log = log;
    nextReport = clock.elapsedMillis() + reportInterval;
    publish(0, 0);
  }

  public void register() {
    if (ACTIVE.putIfAbsent(key, this) != null) {
      throw new IllegalStateException("Diagnostics already registered for Watch ID " + watchId);
    }
  }

  public static Snapshot active(String key) {
    WatchFilesDiagnostics diagnostics = ACTIVE.get(key);
    return diagnostics == null ? null : diagnostics.getSnapshot();
  }

  public void unregister() {
    ACTIVE.remove(key, this);
  }

  public void scan(long start) {
    scans++;
    lastScanAt = clock.wallMillis();
    scanMillis = duration("scan", start);
  }

  public void checkpoint(long start) {
    checkpoints++;
    checkpointMillis = duration("checkpoint", start);
  }

  public void probe(long start) {
    duration("stat", start);
  }

  public void failure(String operation) {
    retries++;
    lastFailure = operation;
  }

  public void overflow(long count) {
    overflow += count;
  }

  public void acknowledged(boolean enabled) {
    if (enabled) emitted++;
    else suppressed++;
  }

  private long duration(String operation, long start) {
    long millis = Math.max(0, clock.elapsedMillis() - start);
    if (millis >= slowMillis) log.accept("Slow " + operation + ": duration_ms=" + millis);
    return millis;
  }

  public void publish(int observed, int pending) {
    snapshot =
        new Snapshot(
            watchId,
            clock.wallMillis(),
            observed,
            pending,
            scans,
            checkpoints,
            retries,
            overflow,
            emitted,
            suppressed,
            lastScanAt,
            scanMillis,
            checkpointMillis,
            lastFailure);
  }

  public void report(int observed, int pending, boolean force) {
    publish(observed, pending);
    long now = clock.elapsedMillis();
    if (force || now >= nextReport) {
      log.accept("Diagnostics " + snapshot);
      nextReport = now + reportInterval;
    }
  }

  public record Snapshot(
      String watchId,
      long sampledAt,
      int observed,
      int pending,
      long scans,
      long checkpoints,
      long retries,
      long overflow,
      long emitted,
      long suppressed,
      long lastSuccessfulScanAt,
      long scanDurationMillis,
      long checkpointDurationMillis,
      String lastFailure) {}
}
