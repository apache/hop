/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.marketplace.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.marketplace.install.IInstallListener;
import org.junit.jupiter.api.Test;

/**
 * The progress adapter is pure arithmetic over callbacks, so it is unit-testable without SWT. What
 * matters: the bar never goes backwards, never exceeds 100, reaches 100 on completion, and UI
 * updates stay throttled no matter how many chunks arrive.
 */
class ProgressMonitorInstallListenerTest {

  /** Captures what the monitor is told, and lets the test drive a fake clock. */
  private static class FakeMonitor implements IProgressMonitor {
    private final List<String> subTasks = new ArrayList<>();
    private int totalWorked;
    private int beginWork;
    private String taskName;
    private boolean cancelled;

    @Override
    public void beginTask(String message, int nrWorks) {
      taskName = message;
      beginWork = nrWorks;
    }

    @Override
    public void subTask(String message) {
      subTasks.add(message);
    }

    @Override
    public boolean isCanceled() {
      return cancelled;
    }

    @Override
    public void worked(int nrWorks) {
      totalWorked += nrWorks;
    }

    @Override
    public void done() {
      // nothing to assert on
    }

    @Override
    public void setTaskName(String taskName) {
      this.taskName = taskName;
    }
  }

  /** Manually advanced clock so throttling is deterministic. */
  private static class FakeClock implements ProgressMonitorInstallListener.IClock {
    private long now;

    @Override
    public long millis() {
      return now;
    }
  }

  @Test
  void singleDownloadDrivesBarToDownloadPhaseCeiling() {
    FakeMonitor monitor = new FakeMonitor();
    FakeClock clock = new FakeClock();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, clock::millis);

    listener.begin("Installing plugin", "Data Vault");
    assertEquals(ProgressMonitorInstallListener.TOTAL_WORK, monitor.beginWork);

    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.started("plugin", 1000L);
    // Halfway through the bytes, with enough clock movement to defeat the throttle.
    clock.now += 1000;
    listener.transferred(500L, 1000L);

    // Download owns 5..85% of a single-item install, so half the bytes is roughly 45%.
    assertTrue(
        monitor.totalWorked >= 40 && monitor.totalWorked <= 50,
        "expected ~45% at half the bytes, got " + monitor.totalWorked);

    clock.now += 1000;
    listener.transferred(1000L, 1000L);
    assertEquals(85, monitor.totalWorked, "all bytes transferred is the end of the download phase");
  }

  @Test
  void completeRunsBarOutToFull() {
    FakeMonitor monitor = new FakeMonitor();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, new FakeClock()::millis);

    listener.begin("Installing plugin", "Data Vault");
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.complete();

    assertEquals(ProgressMonitorInstallListener.TOTAL_WORK, monitor.totalWorked);
  }

  @Test
  void barNeverExceedsTotalOrMovesBackwards() {
    FakeMonitor monitor = new FakeMonitor();
    FakeClock clock = new FakeClock();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, clock::millis);

    listener.begin("Installing plugin", "Data Vault");
    listener.phase(IInstallListener.Phase.ACTIVATE, "x");
    int afterActivate = monitor.totalWorked;
    // A late, out-of-order transfer callback must not drag the bar back.
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    clock.now += 1000;
    listener.transferred(1L, 1000L);

    assertEquals(afterActivate, monitor.totalWorked, "bar moved backwards");

    listener.complete();
    listener.complete();
    assertEquals(
        ProgressMonitorInstallListener.TOTAL_WORK,
        monitor.totalWorked,
        "repeated completion must not push past 100");
  }

  @Test
  void batchScalesEachItemIntoItsOwnSlice() {
    FakeMonitor monitor = new FakeMonitor();
    FakeClock clock = new FakeClock();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, clock::millis);

    listener.begin("Applying environment", null);
    // Fourth of four items: the bar should already be around 75% before this item does any work.
    listener.item("plugin-d", 3, 4);
    assertTrue(
        monitor.totalWorked >= 75 && monitor.totalWorked <= 76,
        "expected ~75% entering item 4 of 4, got " + monitor.totalWorked);
    assertTrue(monitor.taskName.contains("plugin-d"), "task name should name the current item");
    assertTrue(monitor.taskName.contains("4"), "task name should show the batch position");

    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    clock.now += 1000;
    listener.transferred(1000L, 1000L);
    // 75% + 85% of the final quarter ≈ 96%.
    assertTrue(
        monitor.totalWorked >= 95 && monitor.totalWorked <= 97,
        "expected ~96% after the last item downloaded, got " + monitor.totalWorked);
  }

  @Test
  void updatesAreThrottledRegardlessOfChunkCount() {
    FakeMonitor monitor = new FakeMonitor();
    FakeClock clock = new FakeClock();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, clock::millis);

    listener.begin("Installing plugin", "Data Vault");
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.started("plugin", 10_000_000L);
    int subTasksAfterStart = monitor.subTasks.size();

    // 2000 chunks arriving over 1 second of wall clock — what a fast link actually looks like.
    for (int i = 1; i <= 2000; i++) {
      clock.now += 1; // 1ms apart
      listener.transferred(i * 5000L, 10_000_000L);
    }

    int updates = monitor.subTasks.size() - subTasksAfterStart;
    // 2000ms of transfer at a 150ms floor allows ~13 updates. Anything near 2000 means the
    // throttle is broken and we would be flooding the display with asyncExec runnables.
    assertTrue(updates <= 20, "expected the throttle to collapse 2000 chunks, got " + updates);
    assertTrue(updates >= 5, "throttle should still produce visible movement, got " + updates);
  }

  @Test
  void unknownSizeReportsVolumeWithoutPercentOrEta() {
    FakeMonitor monitor = new FakeMonitor();
    FakeClock clock = new FakeClock();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, clock::millis);

    listener.begin("Installing plugin", "Data Vault");
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.started("plugin", -1L);
    int barAtStart = monitor.totalWorked;
    clock.now += 1000;
    listener.transferred(500_000L, -1L);

    assertEquals(barAtStart, monitor.totalWorked, "no size means no percentage to report");
    String last = monitor.subTasks.get(monitor.subTasks.size() - 1);
    assertFalse(last.contains("left"), "an ETA without a total would be invented: " + last);
    assertTrue(last.contains("488KB"), "should show volume downloaded: " + last);
  }

  @Test
  void downloadLineShowsTheDisplayNameNotTheMavenCoordinate() {
    FakeMonitor monitor = new FakeMonitor();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, new FakeClock()::millis);

    listener.begin("Installing Data Vault", "Data Vault");
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    // The download layer only knows the coordinate; it must not leak into the dialog.
    listener.started("org.apache.hop:hop-datavault:2.19.0", 1024L);

    String line = monitor.subTasks.get(monitor.subTasks.size() - 1);
    assertEquals("Downloading: Data Vault", line);
  }

  @Test
  void downloadLineFallsBackToTheTransferLabelWhenNoNameIsKnown() {
    FakeMonitor monitor = new FakeMonitor();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, new FakeClock()::millis);

    listener.begin("Applying environment", null);
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.started("org.postgresql:postgresql:42.7.3", 1024L);

    assertEquals(
        "Downloading: org.postgresql:postgresql:42.7.3",
        monitor.subTasks.get(monitor.subTasks.size() - 1),
        "better to show the coordinate than nothing at all");
  }

  @Test
  void batchItemNameReachesTheDownloadLine() {
    FakeMonitor monitor = new FakeMonitor();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, new FakeClock()::millis);

    listener.begin("Applying environment", null);
    listener.item("hop-tech-parquet", 0, 3);
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.started("org.apache.hop:hop-tech-parquet:2.19.0", 1024L);

    assertEquals(
        "Downloading: hop-tech-parquet", monitor.subTasks.get(monitor.subTasks.size() - 1));
  }

  @Test
  void transferTextReadsLikeABrowserDownload() {
    FakeMonitor monitor = new FakeMonitor();
    FakeClock clock = new FakeClock();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, clock::millis);

    listener.begin("Installing plugin", "Data Vault");
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.started("plugin", 353L * 1024 * 1024);
    // 42MB in 24s ≈ 1.75MB/s, leaving 311MB ≈ 178s ≈ 3 mins.
    clock.now += 24_000;
    listener.transferred(42L * 1024 * 1024, 353L * 1024 * 1024);

    String text = monitor.subTasks.get(monitor.subTasks.size() - 1);
    assertEquals("1.8MB/s - 42.0MB of 353MB, 3 mins left", text);
    assertFalse(text.contains("--"), "the placeholder dashes should be gone");
  }

  @Test
  void etaIsWordedAndSingularisedByUnit() {
    FakeMonitor monitor = new FakeMonitor();
    FakeClock clock = new FakeClock();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, clock::millis);
    listener.begin("Installing plugin", "Data Vault");
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");

    // Drive a known speed of exactly 1000 bytes/s, then vary how much is left.
    listener.started("plugin", 100_000L);
    clock.now += 1000;
    listener.transferred(1000L, 100_000L);

    assertTrue(listener.transferText(99_999L, 100_000L).endsWith("1 sec left"));
    assertTrue(listener.transferText(70_000L, 100_000L).endsWith("30 secs left"));
    assertTrue(listener.transferText(40_000L, 100_000L).endsWith("1 min left"));
    assertTrue(listener.transferText(0L, 3_700_000L).endsWith("1 hr left"));
    assertTrue(listener.transferText(0L, 7_200_000L).endsWith("2 hrs left"));
    // 2.5 hours rounds up rather than truncating to "2 hrs".
    assertTrue(listener.transferText(0L, 9_000_000L).endsWith("3 hrs left"));
  }

  @Test
  void startingIsShownUntilASpeedIsKnown() {
    FakeMonitor monitor = new FakeMonitor();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, new FakeClock()::millis);
    listener.begin("Installing plugin", "Data Vault");
    listener.phase(IInstallListener.Phase.DOWNLOAD, "nexus");
    listener.started("plugin", -1L);

    // No elapsed time yet, so no speed can be measured and no size was advertised.
    assertEquals("Starting download...", listener.transferText(0L, -1L));
    // Size known but still no speed: show the counts rather than a fake rate.
    assertEquals("0B of 1.0KB", listener.transferText(0L, 1024L));
  }

  @Test
  void cancellationIsDelegatedToTheMonitor() {
    FakeMonitor monitor = new FakeMonitor();
    ProgressMonitorInstallListener listener =
        new ProgressMonitorInstallListener(monitor, new FakeClock()::millis);

    assertFalse(listener.isCancelled());
    monitor.cancelled = true;
    assertTrue(listener.isCancelled(), "Cancel button must reach the download loop");
  }
}
