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

import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.install.IInstallListener;
import org.apache.hop.marketplace.install.TransferFormat;

/**
 * Drives an {@link IProgressMonitor} from plugin install progress: the bar shows overall percent
 * across the batch, the subtask line shows the live transfer as "1.8MB/s - 42.0MB of 353MB, 3 mins
 * left".
 *
 * <p>Two things this class exists to get right:
 *
 * <ul>
 *   <li><strong>Throttling.</strong> {@code IInstallListener.transferred} fires once per 64KB chunk
 *       — thousands of times for a large plugin — and {@code IProgressMonitor.subTask} posts an
 *       {@code asyncExec} per call. Updates are collapsed to one per {@link #UPDATE_INTERVAL_MS}.
 *   <li><strong>Scaling.</strong> An install is download + unzip + activate, and a batch is N of
 *       those. Bytes alone would peg the bar at 100% while the unzip still runs, so each phase owns
 *       a slice of its item's share.
 * </ul>
 *
 * <p>All methods are called from the download thread. Only {@link IProgressMonitor} is touched, and
 * that implementation already marshals to the UI thread.
 */
public class ProgressMonitorInstallListener implements IInstallListener {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  /** Total ticks reported to the monitor; the bar is driven in whole percent. */
  public static final int TOTAL_WORK = 100;

  /** Minimum gap between UI updates. Below ~100ms the labels are unreadable anyway. */
  static final long UPDATE_INTERVAL_MS = 150L;

  /**
   * Percent-of-item boundaries for the install phases. Download dominates wall-clock time, so it
   * gets the bulk; the rest keeps the bar moving while jars are written to disk.
   */
  private static final int PCT_AFTER_RESOLVE = 5;

  private static final int PCT_AFTER_DOWNLOAD = 85;
  private static final int PCT_AFTER_UNZIP = 95;

  /** Speed is averaged over this window so the number does not flicker on a bursty link. */
  private static final long SPEED_WINDOW_MS = 2000L;

  private final IProgressMonitor monitor;
  private final IClock clock;

  /** Batch position: item {@code itemIndex} of {@code itemCount}. */
  private int itemIndex;

  private int itemCount = 1;

  /**
   * Short, user-facing name of the current artifact. The download layer only knows the Maven
   * coordinate, which is too noisy for a dialog, so the name is supplied from the UI side instead.
   */
  private String itemName;

  private Phase phase = Phase.RESOLVE;
  private long lastUpdateMs;

  /** Percent already reported to the monitor, so {@code worked()} can be fed deltas. */
  private int reportedPercent;

  private long transferStartMs;
  private long windowStartMs;
  private long windowStartBytes;
  private long lastSpeedBytesPerSec;

  /** Indirection over the clock so throttling and speed can be unit-tested. */
  public interface IClock {
    long millis();
  }

  public ProgressMonitorInstallListener(IProgressMonitor monitor) {
    this(monitor, System::currentTimeMillis);
  }

  ProgressMonitorInstallListener(IProgressMonitor monitor, IClock clock) {
    this.monitor = monitor;
    this.clock = clock;
  }

  /**
   * Call once before the work starts.
   *
   * @param taskName the overall operation name, shown on the persistent task line
   * @param itemName short display name of what is being fetched, used in the download line. May be
   *     null for a batch, where {@link #item(String, int, int)} names each artifact instead.
   */
  public void begin(String taskName, String itemName) {
    this.itemName = itemName;
    monitor.beginTask(taskName, TOTAL_WORK);
  }

  @Override
  public void item(String label, int index, int total) {
    this.itemName = label;
    this.itemIndex = index;
    this.itemCount = Math.max(1, total);
    this.phase = Phase.RESOLVE;
    // The batch position belongs on the persistent task line; the subtask line is reused by every
    // phase and transfer update below, so "3 of 12" would vanish within milliseconds there.
    monitor.setTaskName(
        BaseMessages.getString(
            PKG,
            "MarketplaceDialog.Progress.Item",
            label,
            Integer.toString(index + 1),
            Integer.toString(this.itemCount)));
    // Force the bar forward even if this item turns out to be already satisfied.
    advanceTo(overallPercent(0));
  }

  @Override
  public void phase(Phase newPhase, String detail) {
    this.phase = newPhase;
    advanceTo(overallPercent(phaseFloor(newPhase)));
    monitor.subTask(phaseMessage(newPhase, detail));
    // A new transfer is about to start; reset the speed window.
    if (newPhase == Phase.DOWNLOAD) {
      transferStartMs = 0L;
    }
  }

  @Override
  public void started(String label, long totalBytes) {
    long now = clock.millis();
    transferStartMs = now;
    windowStartMs = now;
    windowStartBytes = 0L;
    lastSpeedBytesPerSec = 0L;
    lastUpdateMs = 0L;
    // `label` is the Maven coordinate (org.apache.hop:hop-x:2.19.0). Show the plugin's name.
    monitor.subTask(
        BaseMessages.getString(
            PKG, "MarketplaceDialog.Progress.Downloading", itemName == null ? label : itemName));
  }

  @Override
  public void transferred(long bytesSoFar, long totalBytes) {
    long now = clock.millis();
    if (now - lastUpdateMs < UPDATE_INTERVAL_MS) {
      return;
    }
    lastUpdateMs = now;

    updateSpeed(now, bytesSoFar);
    if (totalBytes > 0) {
      // Floor, matching the CLI bar: the phase ceiling is only reached on the last chunk.
      int withinDownload = (int) Math.min(100L, bytesSoFar * 100L / totalBytes);
      advanceTo(overallPercent(scaleIntoPhase(withinDownload)));
    }
    monitor.subTask(transferText(bytesSoFar, totalBytes));
  }

  /**
   * The download read-out, laid out like a browser's: {@code 1.7MB/s - 42.0MB of 353MB, 3 mins
   * left}. Parts that cannot be known yet are left out rather than filled with placeholders, so the
   * line grows from "Starting download…" into the full form once a speed has been measured.
   */
  String transferText(long bytesSoFar, long totalBytes) {
    boolean sizeKnown = totalBytes > 0;
    boolean speedKnown = lastSpeedBytesPerSec > 0L;

    if (!speedKnown) {
      return sizeKnown
          ? BaseMessages.getString(
              PKG,
              "MarketplaceDialog.Progress.TransferNoSpeed",
              TransferFormat.bytes(bytesSoFar),
              TransferFormat.bytes(totalBytes))
          : BaseMessages.getString(PKG, "MarketplaceDialog.Progress.Starting");
    }
    if (!sizeKnown) {
      // No Content-Length: an ETA or a percentage would be invented, so report volume and speed.
      return BaseMessages.getString(
          PKG,
          "MarketplaceDialog.Progress.TransferUnknownSize",
          TransferFormat.speed(lastSpeedBytesPerSec),
          TransferFormat.bytes(bytesSoFar));
    }
    long remaining = totalBytes - bytesSoFar;
    if (remaining <= 0) {
      return BaseMessages.getString(
          PKG,
          "MarketplaceDialog.Progress.TransferNoSpeed",
          TransferFormat.bytes(bytesSoFar),
          TransferFormat.bytes(totalBytes));
    }
    return BaseMessages.getString(
        PKG,
        "MarketplaceDialog.Progress.Transfer",
        TransferFormat.speed(lastSpeedBytesPerSec),
        TransferFormat.bytes(bytesSoFar),
        TransferFormat.bytes(totalBytes),
        formatEta(remaining / lastSpeedBytesPerSec));
  }

  @Override
  public boolean isCancelled() {
    return monitor.isCanceled();
  }

  /** Call when the work finished successfully to run the bar out to 100%. */
  public void complete() {
    advanceTo(TOTAL_WORK);
  }

  /**
   * Bytes per second over the trailing {@link #SPEED_WINDOW_MS}, falling back to the average over
   * the whole transfer until the first window closes.
   */
  private void updateSpeed(long now, long bytesSoFar) {
    long windowMs = now - windowStartMs;
    if (windowMs >= SPEED_WINDOW_MS) {
      lastSpeedBytesPerSec = (bytesSoFar - windowStartBytes) * 1000L / windowMs;
      windowStartMs = now;
      windowStartBytes = bytesSoFar;
    } else if (lastSpeedBytesPerSec == 0L) {
      long elapsed = now - transferStartMs;
      if (elapsed > 0) {
        lastSpeedBytesPerSec = bytesSoFar * 1000L / elapsed;
      }
    }
  }

  /** Time remaining in words — "45 secs left", "3 mins left", "2 hrs left". */
  private String formatEta(long seconds) {
    TransferFormat.Eta remaining = TransferFormat.eta(seconds);
    String key =
        switch (remaining.unit()) {
          case SECONDS -> remaining.isSingular() ? "Eta.Second" : "Eta.Seconds";
          case MINUTES -> remaining.isSingular() ? "Eta.Minute" : "Eta.Minutes";
          case HOURS -> remaining.isSingular() ? "Eta.Hour" : "Eta.Hours";
        };
    return BaseMessages.getString(
        PKG, "MarketplaceDialog.Progress." + key, Long.toString(remaining.amount()));
  }

  /** Report the delta needed to reach {@code target}; never moves backwards. */
  private void advanceTo(int target) {
    int clamped = Math.min(TOTAL_WORK, Math.max(0, target));
    if (clamped > reportedPercent) {
      monitor.worked(clamped - reportedPercent);
      reportedPercent = clamped;
    }
  }

  /** Map a 0-100 percent within the current item onto the batch as a whole. */
  private int overallPercent(int percentWithinItem) {
    double perItem = TOTAL_WORK / (double) itemCount;
    return (int) Math.round(itemIndex * perItem + percentWithinItem * perItem / 100.0);
  }

  /** Where a phase starts, as a percentage of one item. */
  private static int phaseFloor(Phase phase) {
    return switch (phase) {
      case RESOLVE -> 0;
      case DOWNLOAD -> PCT_AFTER_RESOLVE;
      case UNZIP -> PCT_AFTER_DOWNLOAD;
      case ACTIVATE -> PCT_AFTER_UNZIP;
    };
  }

  /** Spread a 0-100 download percentage across the download phase's slice of one item. */
  private int scaleIntoPhase(int withinDownload) {
    if (phase != Phase.DOWNLOAD) {
      return phaseFloor(phase);
    }
    int span = PCT_AFTER_DOWNLOAD - PCT_AFTER_RESOLVE;
    return PCT_AFTER_RESOLVE + withinDownload * span / 100;
  }

  private String phaseMessage(Phase phase, String detail) {
    String key =
        switch (phase) {
          case RESOLVE -> "MarketplaceDialog.Progress.Phase.Resolve";
          case DOWNLOAD -> "MarketplaceDialog.Progress.Phase.Download";
          case UNZIP -> "MarketplaceDialog.Progress.Phase.Unzip";
          case ACTIVATE -> "MarketplaceDialog.Progress.Phase.Activate";
        };
    return BaseMessages.getString(PKG, key, detail == null ? "" : detail);
  }
}
