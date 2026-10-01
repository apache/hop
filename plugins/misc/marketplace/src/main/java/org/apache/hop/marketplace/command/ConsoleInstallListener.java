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

package org.apache.hop.marketplace.command;

import java.io.PrintStream;
import org.apache.hop.marketplace.install.IInstallListener;
import org.apache.hop.marketplace.install.TransferFormat;

/**
 * Reports install progress on a terminal with a live progress bar, the way package managers do:
 *
 * <pre>
 * Downloading: Data Vault (353MB)
 *   [████████████░░░░░░░░░░░░]  52%  1.8MB/s - 184MB of 353MB, 1 min left
 * </pre>
 *
 * <p>The bar line is rewritten in place with a carriage return. That only works on a terminal, so
 * when output is redirected to a file or a pipe — a CI log, {@code hop marketplace install >
 * out.txt} — the per-chunk updates are dropped and only the milestone lines are printed. Without
 * that split, a redirected install would write thousands of bar frames into the log.
 *
 * <p>Callbacks arrive on the download thread, but a CLI install has no other thread writing to
 * stdout, so no synchronisation is needed.
 */
public class ConsoleInstallListener implements IInstallListener {

  /** Minimum gap between redraws. Fast enough to look live, slow enough not to flicker. */
  static final long UPDATE_INTERVAL_MS = 200L;

  private static final int BAR_WIDTH = 24;

  /** Speed is averaged over this window so the number does not flicker on a bursty link. */
  private static final long SPEED_WINDOW_MS = 2000L;

  /** Block glyphs, as most modern CLI tools draw a bar. */
  private static final char[] BLOCK_GLYPHS = {'█', '░'};

  /**
   * Fallback for consoles whose encoding cannot represent the blocks (legacy Windows code pages).
   */
  private static final char[] ASCII_GLYPHS = {'#', '-'};

  private final PrintStream out;
  private final boolean interactive;
  private final char[] glyphs;
  private final IClock clock;

  /** Characters written to the current in-place line, so the next redraw can erase it fully. */
  private int openLineWidth;

  private long lastUpdateMs;

  /** Set once the full-bar frame has been drawn, so a repeated final callback is not redrawn. */
  private boolean transferFinished;

  private long transferStartMs;
  private long windowStartMs;
  private long windowStartBytes;
  private long speedBytesPerSec;

  /** Indirection over the clock so throttling and speed can be unit-tested. */
  public interface IClock {
    long millis();
  }

  /**
   * A listener writing to stdout, drawing a live bar only when stdout is a terminal.
   *
   * <p>{@code System.console()} returns null when output is redirected to a file or pipe — the same
   * check {@code ConsoleLoggingEventListener} uses to decide on colour.
   */
  public static ConsoleInstallListener forStdOut() {
    return new ConsoleInstallListener(
        System.out, System.console() != null, glyphsFor(System.out), System::currentTimeMillis);
  }

  ConsoleInstallListener(PrintStream out, boolean interactive, char[] glyphs, IClock clock) {
    this.out = out;
    this.interactive = interactive;
    this.glyphs = glyphs;
    this.clock = clock;
  }

  /**
   * Block glyphs when the stream's encoding can carry them, ASCII otherwise. A cp1252 Windows
   * console would otherwise render the bar as question marks.
   */
  static char[] glyphsFor(PrintStream stream) {
    try {
      return stream.charset().newEncoder().canEncode(BLOCK_GLYPHS[0]) ? BLOCK_GLYPHS : ASCII_GLYPHS;
    } catch (UnsupportedOperationException e) {
      // A charset that cannot produce an encoder; ASCII is always safe.
      return ASCII_GLYPHS;
    }
  }

  @Override
  public void item(String label, int index, int total) {
    if (total > 1) {
      milestone(String.format("[%d/%d] %s", index + 1, total, label));
    }
  }

  @Override
  public void phase(Phase phase, String detail) {
    // RESOLVE is instant and DOWNLOAD is announced by started() with the size, so only the local
    // work worth waiting on gets a line.
    switch (phase) {
      case UNZIP -> milestone("  Unpacking...");
      case ACTIVATE -> milestone("  Installing files...");
      default -> {
        // nothing to announce
      }
    }
  }

  @Override
  public void started(String label, long totalBytes) {
    long now = clock.millis();
    transferStartMs = now;
    windowStartMs = now;
    windowStartBytes = 0L;
    speedBytesPerSec = 0L;
    lastUpdateMs = 0L;
    transferFinished = false;
    milestone(
        totalBytes < 0
            ? "Downloading: " + label
            : "Downloading: " + label + " (" + TransferFormat.bytes(totalBytes) + ")");
  }

  @Override
  public void transferred(long bytesSoFar, long totalBytes) {
    if (!interactive || transferFinished) {
      return;
    }
    long now = clock.millis();
    // The last chunk must never be throttled away: it is what turns the bar into a full 100%, and
    // it closes the line so the next log statement does not land on top of it. Everything else is
    // subject to the redraw floor.
    boolean finished = totalBytes > 0 && bytesSoFar >= totalBytes;
    if (!finished && now - lastUpdateMs < UPDATE_INTERVAL_MS) {
      return;
    }
    lastUpdateMs = now;
    updateSpeed(now, bytesSoFar);
    redraw(progressLine(bytesSoFar, totalBytes));
    if (finished) {
      transferFinished = true;
      endLine();
    }
  }

  /** Call when the work has finished so the in-place bar line is closed off. */
  public void complete() {
    endLine();
  }

  /** The line rewritten in place: bar, percentage, then the same read-out the GUI shows. */
  String progressLine(long bytesSoFar, long totalBytes) {
    StringBuilder sb = new StringBuilder("  ");
    if (totalBytes > 0) {
      // Floor, not round: "100%" alongside "1 sec left" reads as a lie, so only a finished
      // transfer is allowed to show 100.
      int percent = (int) Math.min(100L, bytesSoFar * 100L / totalBytes);
      int filled = percent * BAR_WIDTH / 100;
      sb.append('[');
      sb.append(String.valueOf(glyphs[0]).repeat(filled));
      sb.append(String.valueOf(glyphs[1]).repeat(BAR_WIDTH - filled));
      sb.append(']').append(String.format("%4d%%  ", percent));
    }
    if (speedBytesPerSec > 0L) {
      sb.append(TransferFormat.speed(speedBytesPerSec));
      if (totalBytes > 0) {
        sb.append(" - ")
            .append(TransferFormat.bytes(bytesSoFar))
            .append(" of ")
            .append(TransferFormat.bytes(totalBytes));
        long remaining = totalBytes - bytesSoFar;
        if (remaining > 0) {
          sb.append(", ").append(eta(remaining / speedBytesPerSec));
        }
      } else {
        sb.append(" - ").append(TransferFormat.bytes(bytesSoFar)).append(" downloaded");
      }
    } else if (totalBytes > 0) {
      sb.append(TransferFormat.bytes(bytesSoFar))
          .append(" of ")
          .append(TransferFormat.bytes(totalBytes));
    } else {
      sb.append("starting...");
    }
    return sb.toString();
  }

  private static String eta(long seconds) {
    TransferFormat.Eta remaining = TransferFormat.eta(seconds);
    String unit =
        switch (remaining.unit()) {
          case SECONDS -> remaining.isSingular() ? "sec" : "secs";
          case MINUTES -> remaining.isSingular() ? "min" : "mins";
          case HOURS -> remaining.isSingular() ? "hr" : "hrs";
        };
    return remaining.amount() + " " + unit + " left";
  }

  /**
   * Bytes per second over the trailing window, falling back to the average over the whole transfer
   * until the first window closes.
   */
  private void updateSpeed(long now, long bytesSoFar) {
    long windowMs = now - windowStartMs;
    if (windowMs >= SPEED_WINDOW_MS) {
      speedBytesPerSec = (bytesSoFar - windowStartBytes) * 1000L / windowMs;
      windowStartMs = now;
      windowStartBytes = bytesSoFar;
    } else if (speedBytesPerSec == 0L) {
      long elapsed = now - transferStartMs;
      if (elapsed > 0) {
        speedBytesPerSec = bytesSoFar * 1000L / elapsed;
      }
    }
  }

  /** A line that always gets printed, on its own row. */
  private void milestone(String text) {
    endLine();
    out.println(text);
  }

  /**
   * Overwrite the current line, padding to erase whatever the previous, longer line left behind.
   */
  private void redraw(String text) {
    out.print('\r');
    out.print(text);
    if (openLineWidth > text.length()) {
      out.print(" ".repeat(openLineWidth - text.length()));
    }
    out.flush();
    openLineWidth = text.length();
  }

  /** Finish any in-place line so following output starts on a fresh row. */
  private void endLine() {
    if (openLineWidth > 0) {
      out.println();
      out.flush();
      openLineWidth = 0;
    }
  }
}
